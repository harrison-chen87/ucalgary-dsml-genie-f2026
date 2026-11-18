# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Notebook 4: Create the Genie Space (as code)
# MAGIC
# MAGIC **Why define Genie as code?** A Genie space is only as good as its *context*: which tables it sees, the
# MAGIC business rules in its instructions, and the **trusted example SQL** it can reuse. Keeping that context in a
# MAGIC notebook means every student (and every rehearsal) gets the same, tested space, and changes are reviewable.
# MAGIC
# MAGIC **What it creates** (or updates in place, if a space with the same title already exists):
# MAGIC - **Data:** the daily table, the hourly table, and the 3-2-1 crack spread view, plus the Energy Finance
# MAGIC   ontology from Notebook 3 (entity tables and the metric view) when it exists
# MAGIC - **General instructions:** product synonyms, units, trade-date and roll rules, metric definitions
# MAGIC - **Trusted example SQL:** six vetted queries (volatility, correlation, crack spread, range, volume by hour, rolls)
# MAGIC - **Sample questions** shown to users when they open the space
# MAGIC
# MAGIC Prerequisite: the tables exist, from Notebook 2 (live API) or `00_load_synthetic_data` (no API key needed).

# COMMAND ----------

# MAGIC %run ./_workshop_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "demo", "Schema")
dbutils.widgets.text("space_title", "Energy Futures Analyst (Databento)", "Genie space title")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID (blank = auto)")

CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
SPACE_TITLE = dbutils.widgets.get("space_title")
TABLES = {
    "daily": f"{CATALOG}.{SCHEMA}.{DAILY_TABLE_NAME}",
    "hourly": f"{CATALOG}.{SCHEMA}.{HOURLY_TABLE_NAME}",
    "crack": f"{CATALOG}.{SCHEMA}.{CRACK_VIEW_NAME}",
}
missing = [t for t in TABLES.values() if not spark.catalog.tableExists(t)]
if missing:
    raise RuntimeError(f"Missing tables {missing}. Run Notebook 2 or 00_load_synthetic_data first.")

# The Energy Finance ontology from Notebook 3 is optional: include it when it has been built.
ONTOLOGY = {
    "dim_product": f"{CATALOG}.{SCHEMA}.dim_commodity_product",
    "dim_contract": f"{CATALOG}.{SCHEMA}.dim_futures_contract",
    "dim_session": f"{CATALOG}.{SCHEMA}.dim_trading_session",
    "metrics": f"{CATALOG}.{SCHEMA}.energy_finance_metrics",
}
HAS_ONTOLOGY = all(spark.catalog.tableExists(t) for t in ONTOLOGY.values())
if HAS_ONTOLOGY:
    TABLES.update(ONTOLOGY)
print("Energy Finance ontology:", "included" if HAS_ONTOLOGY else "not built yet (run Notebook 3 to add it)")
print("Tables:", *TABLES.values(), sep="\n  ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. General instructions
# MAGIC These are the "business rules" a new analyst would need on day one. Without them Genie tends to average
# MAGIC prices across products, confuse "gas" with gasoline, and include contract-roll jumps in returns.

# COMMAND ----------

INSTRUCTIONS = f"""## PURPOSE
- Answer questions about CME energy futures prices, volatility, trading volume and refining margins for four products: WTI crude oil, Henry Hub natural gas, RBOB gasoline and NY Harbor ULSD (heating oil / diesel).

## PRODUCT SYNONYMS
- "crude", "oil", "WTI" = product_root 'CL' (USD per barrel).
- "natural gas", "nat gas", "gas" (when not "gasoline"), "Henry Hub" = product_root 'NG' (USD per MMBtu).
- "gasoline", "RBOB", "unleaded" = product_root 'RB' (USD per gallon).
- "diesel", "heating oil", "ULSD", "distillate" = product_root 'HO' (USD per gallon).

## WHICH TABLE TO USE
- Day-level questions (prices, returns, ranges, volatility): {TABLES['daily']}.
- Intraday or hour-of-day questions: {TABLES['hourly']}.
- Refining margin, crack spread, or "gasoline/diesel vs crude" questions: {TABLES['crack']}.

## RULES
- Prices are in each row's price_unit. Never sum or average prices across different products.
- "Day" or "date" means cme_trade_date (the CME session runs 6 pm to 5 pm New York time), never the UTC date.
- For statistics (averages of returns, volatility, correlation, ranges), filter is_complete_session = true. The latest trade date may be a session still in progress.
- Returns: use daily_return_pct and exclude nulls. It is null on contract-roll days because the jump between contract months is not a real price move.
- "Last N days" means the last N trade dates in the data, not calendar days.
- Times of day are New York time (hour_et) unless the user asks for UTC.

## METRIC DEFINITIONS
- Annualized volatility (%) = STDDEV_SAMP(daily_return_pct) * SQRT(252).
- Daily range = high - low (daily_range); relative range = daily_range_pct.
- 3-2-1 crack spread = crack_321_usd_bbl from the crack spread view (estimated refining margin in USD per barrel).
"""

if HAS_ONTOLOGY:
    INSTRUCTIONS += f"""
## ENERGY FINANCE ONTOLOGY
- Product attributes (commodity group, exchange, contract size, tick size) are in {ONTOLOGY['dim_product']}; join on product_root.
- "Refined products" means commodity_group = 'Refined Products' (RBOB gasoline and ULSD). "Crude" products: commodity_group = 'Crude Oil'.
- Contract months are in {ONTOLOGY['dim_contract']} (join on instrument_id); sessions in {ONTOLOGY['dim_session']} (join on cme_trade_date).
- Prefer the governed measures in the metric view {ONTOLOGY['metrics']} for volatility, average range, average close and volume. Query it with MEASURE(`Measure Name`) and GROUP BY its dimensions (`Trade Date`, `Product`, `Commodity Group`, `Day of Week`).
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Trusted example SQL
# MAGIC Each query was tested against the workshop tables. Genie reuses these patterns, so the complex
# MAGIC statistics (volatility, correlation) come out the same way every time.

# COMMAND ----------

EXAMPLE_SQL = [
    ("What is the annualized volatility of each energy product?", """
SELECT product_name,
       COUNT(daily_return_pct) AS return_days,
       ROUND(STDDEV_SAMP(daily_return_pct), 3) AS stdev_daily_return_pct,
       ROUND(STDDEV_SAMP(daily_return_pct) * SQRT(252), 1) AS annualized_volatility_pct
FROM {daily}
WHERE is_complete_session AND daily_return_pct IS NOT NULL
GROUP BY product_name
ORDER BY annualized_volatility_pct DESC"""),
    ("What is the correlation of daily returns between crude oil and gasoline?", """
SELECT ROUND(CORR(c.daily_return_pct, g.daily_return_pct), 3) AS return_correlation,
       COUNT(*) AS trade_days
FROM {daily} c
JOIN {daily} g ON c.cme_trade_date = g.cme_trade_date
WHERE c.product_root = 'CL' AND g.product_root = 'RB'
  AND c.is_complete_session AND g.is_complete_session
  AND c.daily_return_pct IS NOT NULL AND g.daily_return_pct IS NOT NULL"""),
    ("What was the average 3-2-1 crack spread over the last 5 trade dates?", """
SELECT ROUND(AVG(crack_321_usd_bbl), 2) AS avg_crack_321_usd_bbl,
       MIN(cme_trade_date) AS from_date,
       MAX(cme_trade_date) AS to_date
FROM (
  SELECT * FROM {crack}
  WHERE is_complete_session
  ORDER BY cme_trade_date DESC
  LIMIT 5
)"""),
    ("On which trade date did crude oil have its widest daily range?", """
SELECT cme_trade_date, high, low,
       daily_range AS range_usd_per_barrel,
       daily_range_pct
FROM {daily}
WHERE product_root = 'CL' AND is_complete_session
ORDER BY daily_range DESC
LIMIT 1"""),
    ("What is the average hourly volume of crude oil by hour of day in New York time?", """
SELECT hour_et, ROUND(AVG(volume)) AS avg_contracts_per_hour
FROM {hourly}
WHERE product_root = 'CL'
GROUP BY hour_et
ORDER BY hour_et"""),
    ("When did each product roll to a new contract month?", """
SELECT product_name, cme_trade_date AS roll_date, instrument_id AS new_instrument_id
FROM {daily}
WHERE is_contract_roll
ORDER BY cme_trade_date, product_name"""),
]

if HAS_ONTOLOGY:
    EXAMPLE_SQL.append(("What is the annualized volatility of each commodity group?", """
SELECT `Commodity Group`,
       ROUND(MEASURE(`Annualized Volatility Pct`), 1) AS annualized_volatility_pct,
       MEASURE(`Trade Days`) AS trade_days
FROM {metrics}
GROUP BY ALL
ORDER BY annualized_volatility_pct DESC"""))

SAMPLE_QUESTIONS = [
    "Which energy product has been the most volatile over the last 30 days?",
    "Show the daily closing price of natural gas as a line chart.",
    "What was the 3-2-1 crack spread on the most recent complete trade date?",
    "How correlated are crude oil and gasoline daily returns?",
    "Which weekday has the largest average absolute return for gasoline?",
    "At what hour of the day does crude oil trade the most volume?",
]
if HAS_ONTOLOGY:
    SAMPLE_QUESTIONS.append("How volatile are refined products compared with crude oil?")

# Validate every example query before it goes into Genie.
for question, sql in EXAMPLE_SQL:
    spark.sql(sql.format(**TABLES)).limit(1).collect()
print(f"All {len(EXAMPLE_SQL)} example queries ran successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build the serialized space and create or update it
# MAGIC The Genie API takes the whole space as one JSON document (`serialized_space`, version 2). IDs are
# MAGIC deterministic (derived from the content), and lists are sorted by ID as the API expects.

# COMMAND ----------

import json
import uuid

from databricks.sdk import WorkspaceClient

NAMESPACE = uuid.UUID("6f1c2b1e-5d0a-4e8a-9b8f-2c1d0e7a4b31")


def stable_id(text: str) -> str:
    """Deterministic 32-hex ID so re-running produces the same space content."""
    return uuid.uuid5(NAMESPACE, text).hex


def as_lines(text: str) -> list[str]:
    """Split text into the line-list format the Genie API uses."""
    return [line + "\n" for line in text.strip("\n").split("\n")]


serialized = {
    "version": 2,
    "config": {
        "sample_questions": sorted(
            ({"id": stable_id("sq:" + q), "question": [q]} for q in SAMPLE_QUESTIONS), key=lambda x: x["id"]),
    },
    "data_sources": {
        "tables": sorted(({"identifier": t} for t in TABLES.values()), key=lambda x: x["identifier"]),
    },
    "instructions": {
        "text_instructions": [{"id": stable_id("instructions"), "content": as_lines(INSTRUCTIONS)}],
        "example_question_sqls": sorted(
            ({"id": stable_id("ex:" + q), "question": [q], "sql": as_lines(sql.format(**TABLES))}
             for q, sql in EXAMPLE_SQL), key=lambda x: x["id"]),
    },
}

w = WorkspaceClient()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
if not warehouse_id:
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse found; create one or set the warehouse_id widget.")
    warehouse_id = next((wh.id for wh in warehouses if wh.enable_serverless_compute), warehouses[0].id)

description = ("Ask questions about CME energy futures (WTI crude, natural gas, gasoline, diesel): prices, "
               "volatility, correlations, trading volume by hour, contract rolls and the 3-2-1 crack spread. "
               "Data: Databento GLBX.MDP3 hourly bars, built by the Databento Genie workshop notebooks.")

def list_all_spaces() -> list[dict]:
    """List every Genie space, following pagination (the API returns a few spaces per page)."""
    spaces, token = [], None
    while True:
        page = w.api_client.do("GET", "/api/2.0/genie/spaces",
                               query={"page_size": 100, **({"page_token": token} if token else {})})
        spaces += page.get("spaces", [])
        token = page.get("next_page_token")
        if not token:
            return spaces


existing = next((s for s in list_all_spaces() if s.get("title") == SPACE_TITLE), None)
body = {"warehouse_id": warehouse_id, "serialized_space": json.dumps(serialized),
        "title": SPACE_TITLE, "description": description}
if existing:
    space = w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{existing['space_id']}", body=body)
    action = "Updated"
else:
    space = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
    action = "Created"

space_id = space["space_id"]
url = f"{w.config.host.rstrip('/')}/genie/rooms/{space_id}"
print(f"{action} Genie space '{SPACE_TITLE}' ({space_id})\nOpen it: {url}")
displayHTML(f'<a href="{url}" target="_blank">Open the Genie space: {SPACE_TITLE}</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Workshop exercise: what did the instructions buy us?
# MAGIC 1. Ask: *"What is the average price of gas?"* Genie should pick **natural gas (NG)**, not gasoline, and
# MAGIC    answer in USD per MMBtu, because of the synonym rules.
# MAGIC 2. Ask: *"Which product is the most volatile?"* Compare Genie's answer with section 7.1 of Notebook 2;
# MAGIC    they should match, because Genie reuses the trusted volatility SQL.
# MAGIC 3. Ask something we didn't anticipate, e.g. *"Did diesel margins widen after the crude roll?"*, then open
# MAGIC    **Show code** and judge whether the SQL is right. That habit is the key skill for working with Genie.
