# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Notebook 2 - Data Engineering: Ingest 30 Days of Energy Futures for Statistics and Genie
# MAGIC
# MAGIC **Why.** Notebook 1 pulled 10 rows to *discover* the data. Statistics and Genie need *history*: enough
# MAGIC observations to estimate averages, volatility and correlations, and enough rows for natural-language
# MAGIC questions such as "which day had the widest crude oil range?" to have interesting answers.
# MAGIC
# MAGIC **What this notebook builds**
# MAGIC
# MAGIC | Table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | `databento_energy_futures_ohlcv_1h` | one row per symbol per hour | Detailed history; intraday questions |
# MAGIC | `databento_energy_futures_daily` | one row per symbol per **CME trade date** | Day-level statistics; most Genie questions |
# MAGIC
# MAGIC **Same dataset as Notebook 1:** CME Globex (`GLBX.MDP3`) front-month WTI crude (`CL`), natural gas (`NG`),
# MAGIC heating oil (`HO`) and RBOB gasoline (`RB`), now as **hourly** bars (`ohlcv-1h`) for the last 30 days.
# MAGIC That's about 1,900 rows.
# MAGIC
# MAGIC **Two energy-market details this notebook handles**
# MAGIC 1. **Trade date ≠ calendar date.** Globex energy trades Sunday-Friday from 6:00 pm to 5:00 pm New York time.
# MAGIC    A session that opens at 6:00 pm Sunday belongs to *Monday's* trade date, so we derive `cme_trade_date` and
# MAGIC    don't bucket by UTC day.
# MAGIC 2. **Contract rolls.** `CL.c.0` is a *continuous* symbol: it switches to the next contract month at expiry.
# MAGIC    The price jump on a roll is not a real return, so we flag roll days and exclude them from return statistics.
# MAGIC
# MAGIC Re-running the notebook **upserts** (MERGE) into the hourly table, so history accumulates over time.

# COMMAND ----------

# MAGIC %pip install -q databento

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_workshop_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "demo", "Schema")
dbutils.widgets.text("secret_key", "databento_api_key", "UC secret name")
dbutils.widgets.text("hourly_table", "databento_energy_futures_ohlcv_1h", "Hourly table")
dbutils.widgets.text("daily_table", "databento_energy_futures_daily", "Daily table")
dbutils.widgets.text("dataset", "GLBX.MDP3", "Databento dataset")
dbutils.widgets.text("symbols", "CL.c.0,NG.c.0,HO.c.0,RB.c.0", "Symbols (comma-separated)")
dbutils.widgets.dropdown("stype_in", "continuous", ["continuous", "parent", "raw_symbol"], "Symbology")
dbutils.widgets.text("lookback_days", "30", "Lookback days")

params = {name: dbutils.widgets.get(name) for name in [
    "catalog", "schema", "secret_key", "hourly_table", "daily_table", "dataset", "symbols",
    "stype_in", "lookback_days"]}
DATA_SCHEMA = "ohlcv-1h"
SYMBOLS = [s.strip() for s in params["symbols"].split(",") if s.strip()]
HOURLY_TABLE = f"{params['catalog']}.{params['schema']}.{params['hourly_table']}"
DAILY_TABLE = f"{params['catalog']}.{params['schema']}.{params['daily_table']}"
print(f"Hourly table: {HOURLY_TABLE}\nDaily table:  {DAILY_TABLE}\nSymbols: {SYMBOLS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the API key from the Unity Catalog secret

# COMMAND ----------

DATABENTO_API_KEY = dbutils.secrets.get(
    catalog=params["catalog"], schema=params["schema"], key=params["secret_key"]
)

# COMMAND ----------

import databento as db

client = db.Historical(DATABENTO_API_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Engineering - Extract - Pull every hourly bar in the window

# COMMAND ----------

from datetime import timedelta

import pandas as pd

# Each Databento schema publishes on its own schedule, so end the window at the last published hourly bar.
dataset_range = client.metadata.get_dataset_range(dataset=params["dataset"])
schema_range = dataset_range.get("schema", {}).get(DATA_SCHEMA, dataset_range)
end = pd.Timestamp(schema_range["end"]).tz_convert("UTC")
start = end - timedelta(days=int(params["lookback_days"]))
request = dict(dataset=params["dataset"], symbols=SYMBOLS, stype_in=params["stype_in"],
               schema=DATA_SCHEMA, start=start.isoformat(), end=end.isoformat())
print(f"Window: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M} UTC")

pdf = client.timeseries.get_range(**request).to_df()
# Databento uses unsigned integers; Spark has no unsigned types, so cast them to int64.
for col in pdf.select_dtypes(include=["uint8", "uint16", "uint32", "uint64"]).columns:
    pdf[col] = pdf[col].astype("int64")
pdf = pdf.reset_index().drop(columns=[c for c in ("ts_recv", "rtype") if c in pdf.columns])
pdf["ts_event"] = pd.to_datetime(pdf["ts_event"], utc=True).dt.tz_localize(None)
print(f"Pulled {len(pdf):,} hourly bars")
display(pdf.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Engineering - Enrichment - Normalize to ET, Contract Rolls
# MAGIC - `cme_trade_date`: shift New York time forward 6 hours, so the 6:00 pm session open lands on the next date.
# MAGIC - `bar_start_et` / `hour_et`: New York local time, the clock energy traders use.
# MAGIC - `is_contract_roll`: the continuous symbol switched to a new contract month (`instrument_id` changed).

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

by_symbol_time = Window.partitionBy("symbol").orderBy("bar_start_utc")

hourly = (spark.createDataFrame(pdf)
          .withColumnRenamed("ts_event", "bar_start_utc")
          .withColumn("product_root", F.split("symbol", r"\.")[0])
          .join(spark.createDataFrame(PRODUCTS), "product_root", "left")
          .withColumn("bar_start_et", F.from_utc_timestamp("bar_start_utc", "America/New_York"))
          .withColumn("hour_et", F.hour("bar_start_et"))
          .withColumn("cme_trade_date", F.to_date(F.col("bar_start_et") + F.expr("INTERVAL 6 HOURS")))
          .withColumn("is_contract_roll",
                      F.coalesce(F.col("instrument_id") != F.lag("instrument_id").over(by_symbol_time),
                                 F.lit(False)))
          .withColumn("source_dataset", F.lit(params["dataset"]))
          .withColumn("source_schema", F.lit(DATA_SCHEMA))
          .withColumn("ingested_at", F.current_timestamp())
          .select("symbol", "product_root", "product_name", "price_unit", "cme_trade_date",
                  "bar_start_utc", "bar_start_et", "hour_et", "open", "high", "low", "close",
                  F.col("volume").cast("long").alias("volume"), "instrument_id", "is_contract_roll",
                  F.col("publisher_id").cast("int").alias("publisher_id"),
                  "source_dataset", "source_schema", "ingested_at"))
display(hourly.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Engineering - Enrichment - Upsert into the hourly Delta table
# MAGIC The key is `(symbol, bar_start_utc)`. MERGE updates bars we already have (Databento can revise recent bars)
# MAGIC and inserts new ones, so running this notebook again next week extends the history without duplicates.

# COMMAND ----------

if not spark.catalog.tableExists(HOURLY_TABLE):
    hourly.write.format("delta").saveAsTable(HOURLY_TABLE)
    print(f"Created {HOURLY_TABLE}")
else:
    hourly.createOrReplaceTempView("hourly_updates")
    spark.sql(f"""
        MERGE INTO {HOURLY_TABLE} AS t
        USING hourly_updates AS s
        ON t.symbol = s.symbol AND t.bar_start_utc = s.bar_start_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged into {HOURLY_TABLE}")

# Recompute roll flags across the full accumulated history (a new batch may border an older one).
spark.sql(f"""
    MERGE INTO {HOURLY_TABLE} AS t
    USING (
        SELECT symbol, bar_start_utc,
               COALESCE(instrument_id <> LAG(instrument_id) OVER (PARTITION BY symbol ORDER BY bar_start_utc), FALSE)
                 AS is_contract_roll
        FROM {HOURLY_TABLE}
    ) AS s
    ON t.symbol = s.symbol AND t.bar_start_utc = s.bar_start_utc
    WHEN MATCHED AND t.is_contract_roll <> s.is_contract_roll THEN UPDATE SET t.is_contract_roll = s.is_contract_roll
""")

apply_comments(HOURLY_TABLE, HOURLY_TABLE_COMMENT, HOURLY_COLUMN_COMMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Engineering - Enrichment - Build the daily table
# MAGIC Daily open is the first hourly open, close is the last hourly close, high and low are the extremes, and
# MAGIC volume is the sum. `daily_return_pct` is close-to-close and is left **null on roll days**, because the
# MAGIC jump between contract months is not a real price move. `is_complete_session` is false for a session
# MAGIC that hasn't reached its 5 pm ET close yet. We also create the **crack-spread view** used later.
# MAGIC
# MAGIC **What is a crack spread?** A crack spread in oil is the price difference between crude oil and the refined petroleum products (such as gasoline, diesel, and jet fuel) made from it. It serves as a key benchmark for oil refinery profit margins and is also traded as a futures spread to hedge price risk or speculate on energy markets.

# COMMAND ----------

build_daily_table(HOURLY_TABLE, DAILY_TABLE)
CRACK_VIEW = create_crack_spread_view(params["catalog"], params["schema"], DAILY_TABLE)
print(f"{HOURLY_TABLE}: {spark.table(HOURLY_TABLE).count():,} rows")
print(f"{DAILY_TABLE}:  {spark.table(DAILY_TABLE).count():,} rows")
print(f"{CRACK_VIEW}: {spark.table(CRACK_VIEW).count():,} rows (view)")
display(spark.table(DAILY_TABLE).orderBy("cme_trade_date", "symbol"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Science - Basic statistical analysis
# MAGIC ### 7.1 Summary statistics by product
# MAGIC Annualized volatility = standard deviation of daily returns × √252 (trading days per year). It's the
# MAGIC standard way to compare how "jumpy" assets are on the same scale.

# COMMAND ----------

summary = spark.sql(f"""
    SELECT product_name,
           price_unit,
           COUNT(*)                                        AS trade_days,
           MIN(cme_trade_date)                             AS first_day,
           MAX(cme_trade_date)                             AS last_day,
           ROUND(AVG(close), 4)                            AS avg_close,
           ROUND(MIN(low), 4)                              AS min_low,
           ROUND(MAX(high), 4)                             AS max_high,
           ROUND(AVG(daily_return_pct), 3)                 AS avg_daily_return_pct,
           ROUND(STDDEV_SAMP(daily_return_pct), 3)         AS stdev_daily_return_pct,
           ROUND(STDDEV_SAMP(daily_return_pct) * SQRT(252), 1) AS annualized_volatility_pct,
           ROUND(AVG(daily_range_pct), 3)                  AS avg_daily_range_pct,
           ROUND(AVG(volume))                              AS avg_daily_volume
    FROM {DAILY_TABLE}
    WHERE is_complete_session
    GROUP BY product_name, price_unit
    ORDER BY annualized_volatility_pct DESC
""")
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 How do the products move together? (correlation of daily returns)
# MAGIC Crude oil is the feedstock for gasoline and diesel, so we expect CL, RB and HO to be strongly correlated.
# MAGIC Natural gas has different supply and demand drivers (weather, storage, LNG exports) and usually moves more
# MAGIC independently.

# COMMAND ----------

returns_wide = (spark.table(DAILY_TABLE).where("is_complete_session")
                .groupBy("cme_trade_date").pivot("product_root").agg(F.first("daily_return_pct"))
                .toPandas().set_index("cme_trade_date").sort_index())
display(returns_wide.corr().round(3).reset_index().rename(columns={"index": "product_root"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 When does the market trade? (average volume by hour, New York time)
# MAGIC Liquidity concentrates around the old floor ("pit") hours, roughly 9:00 am to 2:30 pm ET, and around the
# MAGIC Wednesday EIA petroleum inventory report at 10:30 am ET.

# COMMAND ----------

display(spark.sql(f"""
    SELECT hour_et, product_root, ROUND(AVG(volume)) AS avg_hourly_volume
    FROM {HOURLY_TABLE}
    GROUP BY hour_et, product_root
    ORDER BY hour_et, product_root
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 A refining view: the 3-2-1 crack spread
# MAGIC Refiners turn 3 barrels of crude into roughly 2 barrels of gasoline and 1 of diesel. The **3-2-1 crack
# MAGIC spread** estimates the refining margin in USD per barrel. RB and HO are quoted per gallon, so we multiply by
# MAGIC 42 gallons per barrel:
# MAGIC
# MAGIC `crack = (2 × RB × 42 + 1 × HO × 42 − 3 × CL) / 3`
# MAGIC
# MAGIC It's saved as the view `databento_crack_spread_daily`, so Genie can answer "what was the refining margin?"
# MAGIC by looking it up rather than rebuilding the formula.

# COMMAND ----------

display(spark.table(CRACK_VIEW).orderBy("cme_trade_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Hand off to Genie
# MAGIC Run **`03_energy_finance_domain`** to model the Energy Finance ontology, then **`04_create_genie_space`**.
# MAGIC Notebook 4 creates (or updates) a Genie space over the daily table, the hourly table
# MAGIC and the crack-spread view, with general instructions, trusted example SQL and sample questions, all defined
# MAGIC as code.
