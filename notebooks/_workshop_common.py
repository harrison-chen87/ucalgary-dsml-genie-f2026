# Databricks notebook source
# MAGIC %md
# MAGIC # Shared workshop helpers
# MAGIC Loaded with `%run ./_workshop_common` by Notebook 2, the synthetic-data loader, and the Genie setup notebook.
# MAGIC Keeps table and column comments, the product reference data, the daily-table rules and the crack-spread view in
# MAGIC **one** place, so both paths to the tables (live API or synthetic CSV) produce identical, Genie-ready objects.

# COMMAND ----------

import pandas as pd

HOURLY_TABLE_NAME = "databento_energy_futures_ohlcv_1h"
DAILY_TABLE_NAME = "databento_energy_futures_daily"
CRACK_VIEW_NAME = "databento_crack_spread_daily"

PRODUCTS = pd.DataFrame([
    ("CL", "WTI Crude Oil", "USD per barrel"),
    ("NG", "Henry Hub Natural Gas", "USD per MMBtu"),
    ("HO", "NY Harbor ULSD (Heating Oil)", "USD per gallon"),
    ("RB", "RBOB Gasoline", "USD per gallon"),
], columns=["product_root", "product_name", "price_unit"])

HOURLY_TABLE_COMMENT = (
    "Hourly OHLCV bars for CME Globex energy futures (front-month continuous contracts: WTI crude oil CL, "
    "Henry Hub natural gas NG, NY Harbor ULSD heating oil HO, RBOB gasoline RB) from Databento GLBX.MDP3 ohlcv-1h. "
    "One row per symbol per hour that had trades (hours without trades have no row). Use cme_trade_date "
    "(not the UTC date) for day-level questions. Prices are in each product's price_unit.")

HOURLY_COLUMN_COMMENTS = {
    "symbol": "Databento continuous symbol, e.g. CL.c.0 = front-month WTI crude oil (calendar roll).",
    "product_root": "Futures product code: CL, NG, HO or RB.",
    "product_name": "Human-readable product name.",
    "price_unit": "Unit for open/high/low/close, e.g. USD per barrel.",
    "cme_trade_date": "CME trade date. Sessions open at 6 pm New York time and belong to the next date.",
    "bar_start_utc": "Start of the one-hour bar in UTC.",
    "bar_start_et": "Start of the one-hour bar in New York (US Eastern) time.",
    "hour_et": "Hour of day (0-23) of bar start in New York time.",
    "open": "First traded price in the hour.", "high": "Highest traded price in the hour.",
    "low": "Lowest traded price in the hour.", "close": "Last traded price in the hour.",
    "volume": "Contracts traded during the hour.",
    "instrument_id": "Databento ID of the specific contract month the continuous symbol pointed to.",
    "is_contract_roll": "True on the first bar after the continuous symbol rolled to a new contract month.",
    "publisher_id": "Databento publisher (venue/feed) ID.",
    "source_dataset": "Databento dataset code.", "source_schema": "Databento record schema.",
    "ingested_at": "When this row was last written to Databricks.",
}

DAILY_TABLE_COMMENT = (
    "Daily OHLCV summary for CME energy futures (WTI crude CL, natural gas NG, heating oil HO, RBOB gasoline RB), "
    "aggregated from the hourly table by CME trade date. One row per product per trade date. "
    "daily_return_pct is null on contract roll days and on each symbol's first day. "
    "Filter is_complete_session = true for statistics; the latest trade date may still be in progress.")

DAILY_COLUMN_COMMENTS = {
    "symbol": "Databento continuous symbol, e.g. CL.c.0.",
    "product_root": "Futures product code: CL, NG, HO or RB.",
    "product_name": "Human-readable product name.",
    "price_unit": "Unit for prices, e.g. USD per barrel.",
    "cme_trade_date": "CME trade date (session runs 6 pm prior day to 5 pm New York time).",
    "open": "Opening price of the trade date (first hourly open).",
    "high": "Highest price of the trade date.", "low": "Lowest price of the trade date.",
    "close": "Closing price of the trade date (last hourly close; a proxy for settlement).",
    "volume": "Total contracts traded on the trade date.",
    "hourly_bar_count": "Number of hourly bars with trades. Low counts are normal for illiquid hours.",
    "instrument_id": "Databento ID of the contract month at the close.",
    "is_contract_roll": "True if the continuous symbol rolled to a new contract month on this date.",
    "is_complete_session": "True if the data covers this trade date through its 5 pm New York close. "
                           "False for a session still in progress; exclude those rows from statistics.",
    "daily_return_pct": "Close-to-close percent change vs. prior trade date; null on roll days.",
    "daily_range": "High minus low, in price_unit.",
    "daily_range_pct": "High minus low as a percent of the open (a simple volatility measure).",
    "day_of_week": "Weekday name of the trade date.",
    "updated_at": "When this daily table was last rebuilt.",
}


def sql_literal(text: str) -> str:
    """Quote a Python string as a Spark SQL string literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def apply_comments(table: str, table_comment: str, column_comments: dict) -> None:
    """Set table and column comments; Genie reads these to understand the data."""
    spark.sql(f"COMMENT ON TABLE {table} IS {sql_literal(table_comment)}")
    existing = {f.name for f in spark.table(table).schema.fields}
    for column, text in column_comments.items():
        if column in existing:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT {sql_literal(text)}")


def drop_ontology_constraints(table: str) -> None:
    """Remove the primary/foreign keys Notebook 3 adds, so the table can be rewritten (Notebook 3 re-adds them)."""
    if not spark.catalog.tableExists(table):
        return
    prefix = "daily" if table.endswith(DAILY_TABLE_NAME) else "hourly"
    for target in ("product", "contract", "session"):
        spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {prefix}_fk_{target}")
    spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {prefix}_pk")


def build_daily_table(hourly_table: str, daily_table: str) -> None:
    """(Re)build the daily table from the hourly table: one row per symbol per CME trade date.

    open = first hourly open, close = last hourly close, high/low = extremes, volume = sum.
    daily_return_pct is close-to-close and null on contract-roll days. is_complete_session is true once the
    data reaches the trade date's 5 pm New York close.
    """
    from datetime import timedelta

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    hourly_all = spark.table(hourly_table)
    # End of the data window in New York time = start of the latest bar + 1 hour.
    data_end_et = (hourly_all.agg(F.max("bar_start_et")).first()[0]) + timedelta(hours=1)
    by_symbol_day = Window.partitionBy("symbol").orderBy("cme_trade_date")

    daily = (hourly_all.groupBy("symbol", "product_root", "product_name", "price_unit", "cme_trade_date")
             .agg(F.min_by("open", "bar_start_utc").alias("open"),
                  F.max("high").alias("high"),
                  F.min("low").alias("low"),
                  F.max_by("close", "bar_start_utc").alias("close"),
                  F.sum("volume").alias("volume"),
                  F.count("*").alias("hourly_bar_count"),
                  F.max_by("instrument_id", "bar_start_utc").alias("instrument_id"))
             .withColumn("prev_close", F.lag("close").over(by_symbol_day))
             .withColumn("prev_instrument_id", F.lag("instrument_id").over(by_symbol_day))
             .withColumn("is_contract_roll", F.coalesce(F.col("instrument_id") != F.col("prev_instrument_id"),
                                                        F.lit(False)))
             .withColumn("daily_return_pct",
                         F.when(~F.col("is_contract_roll"),
                                F.round((F.col("close") / F.col("prev_close") - 1) * 100, 4)))
             .withColumn("daily_range", F.round(F.col("high") - F.col("low"), 6))
             .withColumn("daily_range_pct", F.round((F.col("high") - F.col("low")) / F.col("open") * 100, 4))
             .withColumn("day_of_week", F.date_format("cme_trade_date", "EEEE"))
             # A session is complete once the data reaches its 5 pm New York close.
             .withColumn("is_complete_session",
                         F.lit(data_end_et) >= F.to_timestamp(F.concat(F.col("cme_trade_date").cast("string"),
                                                                       F.lit(" 17:00:00"))))
             .drop("prev_close", "prev_instrument_id")
             .withColumn("updated_at", F.current_timestamp()))

    drop_ontology_constraints(daily_table)
    daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(daily_table)
    apply_comments(daily_table, DAILY_TABLE_COMMENT, DAILY_COLUMN_COMMENTS)


def create_crack_spread_view(catalog: str, schema: str, daily_table: str | None = None) -> str:
    """Create the 3-2-1 crack spread view over the daily table and return its full name.

    crack (USD/bbl) = (2 x RBOB x 42 + 1 x ULSD x 42 - 3 x WTI) / 3, using daily closes.
    RBOB and ULSD are quoted per gallon; 42 gallons = 1 barrel.
    """
    daily = daily_table or f"{catalog}.{schema}.{DAILY_TABLE_NAME}"
    view = f"{catalog}.{schema}.{CRACK_VIEW_NAME}"
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view} (
            cme_trade_date COMMENT 'CME trade date.',
            wti_close_usd_bbl COMMENT 'WTI crude oil (CL) close, USD per barrel.',
            rbob_close_usd_gal COMMENT 'RBOB gasoline (RB) close, USD per gallon.',
            ulsd_close_usd_gal COMMENT 'NY Harbor ULSD heating oil (HO) close, USD per gallon.',
            rbob_crack_usd_bbl COMMENT 'Gasoline crack: RBOB x 42 minus WTI, USD per barrel.',
            ulsd_crack_usd_bbl COMMENT 'Diesel (heat) crack: ULSD x 42 minus WTI, USD per barrel.',
            crack_321_usd_bbl COMMENT '3-2-1 crack spread (estimated refining margin), USD per barrel.',
            any_contract_roll COMMENT 'True if any of the three legs rolled contract month on this date.',
            is_complete_session COMMENT 'True if all three legs have a complete session on this date.'
        )
        COMMENT {sql_literal(
            "Daily refining margin proxies from CME energy futures closes. The 3-2-1 crack spread estimates "
            "the gross margin of refining 3 barrels of WTI crude into 2 barrels of gasoline and 1 barrel of "
            "diesel, in USD per barrel. One row per CME trade date where all three legs traded.")}
        AS
        WITH legs AS (
            SELECT cme_trade_date,
                   MAX(CASE WHEN product_root = 'CL' THEN close END) AS wti,
                   MAX(CASE WHEN product_root = 'RB' THEN close END) AS rbob,
                   MAX(CASE WHEN product_root = 'HO' THEN close END) AS ulsd,
                   BOOL_OR(is_contract_roll) AS any_roll,
                   BOOL_AND(is_complete_session) AS complete,
                   COUNT(*) AS legs
            FROM {daily}
            WHERE product_root IN ('CL', 'RB', 'HO')
            GROUP BY cme_trade_date
        )
        SELECT cme_trade_date, wti, rbob, ulsd,
               ROUND(rbob * 42 - wti, 2),
               ROUND(ulsd * 42 - wti, 2),
               ROUND((2 * rbob * 42 + ulsd * 42 - 3 * wti) / 3, 2),
               any_roll, complete
        FROM legs
        WHERE legs = 3
    """)
    return view
