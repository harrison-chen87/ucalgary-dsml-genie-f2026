# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Notebook 0: Load the Synthetic Dataset (no API key needed)
# MAGIC
# MAGIC **Use this if** you don't have a Databento API key, or want to go straight to the Genie part of the workshop.
# MAGIC It loads the CSVs in this repo's `data/synthetic/` folder and builds the **same** tables and view as Notebook 2,
# MAGIC so Notebook 3 (ontology) and Notebook 4 (Genie space) work unchanged.
# MAGIC
# MAGIC **What is this data?** It is **simulated**, not real market prices. A model was fitted to the *statistics* of
# MAGIC 30 days of real CME energy futures data (each product's volatility, how the products move together, volume by
# MAGIC hour, typical daily range, and the price gap at contract rolls), then used to generate 60 new trade dates of
# MAGIC hourly bars. It follows the real market's rules: 6 pm to 5 pm ET sessions, the CME roll calendar, and tick sizes.
# MAGIC Rows are labelled `source_dataset = 'SYNTHETIC'`.
# MAGIC
# MAGIC | Object | Built from |
# MAGIC |---|---|
# MAGIC | `databento_energy_futures_ohlcv_1h` | `data/synthetic/energy_futures_hourly.csv` |
# MAGIC | `databento_energy_futures_daily` | the hourly table (same logic as Notebook 2) |
# MAGIC | `databento_crack_spread_daily` (view) | the daily table |

# COMMAND ----------

# MAGIC %run ./_workshop_common

# COMMAND ----------

import os

import pandas as pd
from pyspark.sql import functions as F

RAW_GITHUB = "https://raw.githubusercontent.com/harrison-chen87/ucalgary-dsml-genie-f2026/main/data/synthetic"

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "demo", "Schema")
dbutils.widgets.text("data_path", "", "CSV folder or URL (blank = this repo)")
dbutils.widgets.dropdown("replace_real_data", "no", ["no", "yes"], "Replace tables built from real data?")

CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
HOURLY_TABLE = f"{CATALOG}.{SCHEMA}.{HOURLY_TABLE_NAME}"
DAILY_TABLE = f"{CATALOG}.{SCHEMA}.{DAILY_TABLE_NAME}"


def default_data_path() -> str:
    """data/synthetic next to this notebook's Git folder, or the public GitHub copy as a fallback."""
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}"))
    local = os.path.join(repo_root, "data", "synthetic")
    return local if os.path.exists(os.path.join(local, "energy_futures_hourly.csv")) else RAW_GITHUB


DATA_PATH = dbutils.widgets.get("data_path").strip().rstrip("/") or default_data_path()
print(f"Loading synthetic data from: {DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Safety check
# MAGIC If the target schema already holds tables built from the **real** API (Notebook 2), stop rather than overwrite them,
# MAGIC unless `replace_real_data = yes`. You can also point the `schema` widget at a separate schema.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
if spark.catalog.tableExists(HOURLY_TABLE) and dbutils.widgets.get("replace_real_data") != "yes":
    real_rows = spark.table(HOURLY_TABLE).where(F.col("source_dataset") != "SYNTHETIC").limit(1).count()
    if real_rows:
        raise RuntimeError(f"{HOURLY_TABLE} contains real Databento data. Set replace_real_data = yes to replace it, "
                           "or use a different schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the hourly CSV, then build the daily table and crack-spread view

# COMMAND ----------

hourly_pdf = pd.read_csv(f"{DATA_PATH}/energy_futures_hourly.csv",
                         parse_dates=["bar_start_utc", "bar_start_et"])
hourly_pdf["cme_trade_date"] = pd.to_datetime(hourly_pdf["cme_trade_date"]).dt.date
print(f"Read {len(hourly_pdf):,} hourly rows, trade dates {hourly_pdf['cme_trade_date'].min()} "
      f"to {hourly_pdf['cme_trade_date'].max()}")

hourly = (spark.createDataFrame(hourly_pdf)
          .withColumn("hour_et", F.col("hour_et").cast("int"))
          .withColumn("volume", F.col("volume").cast("long"))
          .withColumn("instrument_id", F.col("instrument_id").cast("long"))
          .withColumn("publisher_id", F.col("publisher_id").cast("int"))
          .withColumn("ingested_at", F.current_timestamp()))
drop_ontology_constraints(HOURLY_TABLE)
hourly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(HOURLY_TABLE)

SYNTHETIC_NOTE = "SYNTHETIC DATA: simulated from statistics of real CME data, not real market prices. "
apply_comments(HOURLY_TABLE, SYNTHETIC_NOTE + HOURLY_TABLE_COMMENT, HOURLY_COLUMN_COMMENTS)
build_daily_table(HOURLY_TABLE, DAILY_TABLE)
spark.sql(f"COMMENT ON TABLE {DAILY_TABLE} IS {sql_literal(SYNTHETIC_NOTE + DAILY_TABLE_COMMENT)}")
CRACK_VIEW = create_crack_spread_view(CATALOG, SCHEMA, DAILY_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check against the published daily CSV
# MAGIC The repo also ships `energy_futures_daily.csv`, built by the same rules outside Databricks. The table built here
# MAGIC should match it exactly.

# COMMAND ----------

expected = pd.read_csv(f"{DATA_PATH}/energy_futures_daily.csv")
built = spark.table(DAILY_TABLE).select("symbol", "cme_trade_date", "close", "daily_return_pct").toPandas()
built["cme_trade_date"] = built["cme_trade_date"].astype(str)
merged = expected.merge(built, on=["symbol", "cme_trade_date"], suffixes=("_csv", "_table"))
if len(merged) != len(expected) or len(built) != len(expected):
    raise ValueError(f"Daily row mismatch: CSV {len(expected)}, table {len(built)}, matched {len(merged)}")
if (merged["close_csv"] - merged["close_table"]).abs().max() > 1e-9:
    raise ValueError("Daily closes differ between the CSV and the table")
print(f"{HOURLY_TABLE}: {spark.table(HOURLY_TABLE).count():,} rows")
print(f"{DAILY_TABLE}:  {len(built):,} rows (matches energy_futures_daily.csv)")
print(f"{CRACK_VIEW}: {spark.table(CRACK_VIEW).count():,} rows (view)")
print("Next: run 03_energy_finance_domain, then 04_create_genie_space.")

# COMMAND ----------

display(spark.table(DAILY_TABLE).orderBy("cme_trade_date", "symbol"))
