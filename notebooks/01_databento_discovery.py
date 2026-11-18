# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Databento → Unity Catalog: Data Discovery for a Genie Workshop
# MAGIC
# MAGIC **Why this notebook exists.** Genie answers questions by writing SQL against *governed tables* in
# MAGIC Unity Catalog. It cannot call an external API directly. So the first step of any "Genie over an API"
# MAGIC demo is to land a sample of the API's data in a Delta table, with good comments, and inspect it.
# MAGIC
# MAGIC **What this notebook does**
# MAGIC 1. Reads the Databento API key from a **Unity Catalog secret** (`workspace.demo.databento_api_key`). The key is never printed.
# MAGIC 2. Uses Databento's free **metadata** endpoints to discover datasets, schemas, and available date ranges.
# MAGIC 3. Pulls **10 records** of market data into a pandas DataFrame, then a Spark DataFrame.
# MAGIC 4. Writes them to a new Delta table with table and column comments, which Genie uses to understand the data.
# MAGIC
# MAGIC **Default example: the energy futures complex on CME Globex (`GLBX.MDP3`).**
# MAGIC Front-month continuous contracts for WTI crude oil (`CL`), Henry Hub natural gas (`NG`), ULSD heating oil (`HO`)
# MAGIC and RBOB gasoline (`RB`), as daily OHLCV bars (open, high, low, close, volume).
# MAGIC
# MAGIC | Databento concept | Meaning | Example |
# MAGIC |---|---|---|
# MAGIC | Dataset | A venue's feed | `GLBX.MDP3` = CME Globex, `XNAS.ITCH` = Nasdaq |
# MAGIC | Schema | The record shape / granularity | `ohlcv-1d` daily bars, `trades` tick trades, `mbp-1` top of book |
# MAGIC | Symbology (`stype_in`) | How you name instruments | `continuous` → `CL.c.0` = front-month crude by calendar roll |

# COMMAND ----------

# MAGIC %pip install -q databento

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC Change these with the widgets at the top of the notebook. To use equities instead, try
# MAGIC dataset `XNAS.ITCH`, symbols `AAPL,MSFT,NVDA`, stype `raw_symbol`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "demo", "Schema")
dbutils.widgets.text("table", "databento_energy_futures_ohlcv_1d", "Target table")
dbutils.widgets.text("secret_full_name", "workspace.demo.databento_api_key", "UC secret")
dbutils.widgets.text("dataset", "GLBX.MDP3", "Databento dataset")
dbutils.widgets.text("symbols", "CL.c.0,NG.c.0,HO.c.0,RB.c.0", "Symbols (comma-separated)")
dbutils.widgets.dropdown("stype_in", "continuous", ["continuous", "parent", "raw_symbol"], "Symbology")
dbutils.widgets.text("data_schema", "ohlcv-1d", "Databento schema")
dbutils.widgets.text("lookback_days", "30", "Lookback days")
dbutils.widgets.text("limit", "10", "Max records")

params = {name: dbutils.widgets.get(name) for name in [
    "catalog", "schema", "table", "secret_full_name", "dataset", "symbols",
    "stype_in", "data_schema", "lookback_days", "limit"]}
SYMBOLS = [s.strip() for s in params["symbols"].split(",") if s.strip()]
LIMIT = int(params["limit"])
TARGET_TABLE = f"{params['catalog']}.{params['schema']}.{params['table']}"
print(f"Target table: {TARGET_TABLE}\nSymbols: {SYMBOLS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the API key from the Unity Catalog secret
# MAGIC UC secrets are governed like tables: they live at `catalog.schema.secret_name` and access is controlled
# MAGIC with Unity Catalog grants. Read them with `dbutils.secrets.get(catalog=..., schema=..., key=...)`.
# MAGIC
# MAGIC The keyword arguments matter: the positional form `dbutils.secrets.get(scope, key)` looks up a classic
# MAGIC workspace secret scope and won't find a UC secret. This notebook pins serverless environment version 6
# MAGIC (see the header) so the UC-aware `dbutils` is available.

# COMMAND ----------

DATABENTO_API_KEY = dbutils.secrets.get(
    catalog="workspace", schema="demo", key="databento_api_key"
)

# COMMAND ----------

import databento as db

client = db.Historical(DATABENTO_API_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Discover what's available (metadata calls)

# COMMAND ----------

import pandas as pd

datasets = client.metadata.list_datasets()
print(f"{len(datasets)} datasets available to this key")
display(pd.DataFrame({"dataset": datasets}))

# COMMAND ----------

schemas = client.metadata.list_schemas(dataset=params["dataset"])
display(pd.DataFrame({"schema": schemas}))

# COMMAND ----------

from datetime import datetime, timedelta, timezone

dataset_range = client.metadata.get_dataset_range(dataset=params["dataset"])
# Each schema has its own availability window; daily bars lag the live feed until the session closes.
schema_range = dataset_range.get("schema", {}).get(params["data_schema"], dataset_range)
available_end = pd.Timestamp(schema_range["end"]).tz_convert("UTC")
end = min(available_end, pd.Timestamp(datetime.now(timezone.utc)))
start = end - timedelta(days=int(params["lookback_days"]))
print(f"{params['dataset']} / {params['data_schema']} history: {schema_range['start']} → {schema_range['end']}")
print(f"Requesting window: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M} UTC")

# COMMAND ----------

request = dict(dataset=params["dataset"], symbols=SYMBOLS, stype_in=params["stype_in"],
               schema=params["data_schema"], start=start.isoformat(), end=end.isoformat(), limit=LIMIT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pull the records into a DataFrame
# MAGIC `to_df()` converts prices from Databento's fixed-point integers into decimals and maps
# MAGIC `instrument_id` back to the symbol you requested.

# COMMAND ----------

store = client.timeseries.get_range(**request)
pdf = store.to_df()
# Databento uses unsigned integers (uint8/16/32/64). Spark (and display()) have no unsigned types,
# so cast them to int64 before anything touches Spark.
for col in pdf.select_dtypes(include=["uint8", "uint16", "uint32", "uint64"]).columns:
    pdf[col] = pdf[col].astype("int64")
print(f"Pulled {len(pdf)} records; columns: {list(pdf.columns)}")
display(pdf.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Shape it for Genie and write a Delta table
# MAGIC Genie works best with clear names, proper types, and comments. We:
# MAGIC - turn the `ts_event` index into a `trade_date` (daily bars) plus a UTC timestamp,
# MAGIC - add a readable `product_name` for each futures root,
# MAGIC - add lineage columns (`source_dataset`, `ingested_at`).

# COMMAND ----------

from pyspark.sql import functions as F

PRODUCT_NAMES = {
    "CL": "WTI Crude Oil", "NG": "Henry Hub Natural Gas", "HO": "NY Harbor ULSD (Heating Oil)",
    "RB": "RBOB Gasoline", "BZ": "Brent Crude Oil", "ES": "E-mini S&P 500",
}

out = pdf.reset_index()
out["ts_event"] = pd.to_datetime(out["ts_event"], utc=True).dt.tz_localize(None)
out = out.drop(columns=[c for c in ("ts_recv", "rtype") if c in out.columns])
out["product_root"] = out["symbol"].str.split(".").str[0]
out["product_name"] = out["product_root"].map(PRODUCT_NAMES).fillna(out["product_root"])

sdf = (spark.createDataFrame(out)
       .withColumn("trade_date", F.to_date("ts_event"))
       .withColumn("source_dataset", F.lit(params["dataset"]))
       .withColumn("source_schema", F.lit(params["data_schema"]))
       .withColumn("ingested_at", F.current_timestamp())
       .select("trade_date", F.col("ts_event").alias("bar_start_utc"), "symbol", "product_root",
               "product_name", "open", "high", "low", "close", F.col("volume").cast("long").alias("volume"),
               F.col("instrument_id").cast("long").alias("instrument_id"),
               F.col("publisher_id").cast("int").alias("publisher_id"),
               "source_dataset", "source_schema", "ingested_at"))
display(sdf)

# COMMAND ----------

def sql_literal(text: str) -> str:
    """Quote a Python string as a Spark SQL string literal (escapes backslashes and quotes)."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


(sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

table_comment = (f"Sample of Databento {params['data_schema']} market data from {params['dataset']} "
                 f"for {', '.join(SYMBOLS)} (stype {params['stype_in']}). One row per symbol per bar. "
                 "Prices are in the contract's quoted units (e.g. USD/barrel for CL, USD/MMBtu for NG). "
                 "Loaded by the Databento Genie workshop discovery notebook.")
spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS {sql_literal(table_comment)}")

COLUMN_COMMENTS = {
    "trade_date": "Trading date of the bar (UTC date of bar start).",
    "bar_start_utc": "Bar open timestamp in UTC (Databento ts_event).",
    "symbol": "Requested symbol, e.g. CL.c.0 = front-month WTI crude by calendar roll.",
    "product_root": "Futures product root code (CL, NG, HO, RB).",
    "product_name": "Human-readable product name.",
    "open": "First traded price in the bar.", "high": "Highest traded price in the bar.",
    "low": "Lowest traded price in the bar.", "close": "Last traded price in the bar.",
    "volume": "Number of contracts traded in the bar.",
    "instrument_id": "Databento numeric instrument ID of the specific contract month.",
    "publisher_id": "Databento publisher (venue/feed) ID.",
    "source_dataset": "Databento dataset code.", "source_schema": "Databento record schema.",
    "ingested_at": "Time this row was loaded into Databricks.",
}
for col, text in COLUMN_COMMENTS.items():
    spark.sql(f"ALTER TABLE {TARGET_TABLE} ALTER COLUMN {col} COMMENT {sql_literal(text)}")
print(f"Wrote {sdf.count()} rows to {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify the table

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY trade_date, symbol"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {TARGET_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps for the workshop
# MAGIC - Notebook 2 pulls 30 days of hourly bars to give statistics and Genie enough history.
# MAGIC - Create a Genie space on this table and try: *"What was the average daily range (high minus low) of crude oil?"*,
# MAGIC   *"Which energy product had the highest volume?"*, *"Show the closing price trend for natural gas."*