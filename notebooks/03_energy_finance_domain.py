# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Notebook 3 - Ontology: Build the Energy Finance Domain
# MAGIC
# MAGIC ## What is an ontology?
# MAGIC An **ontology** is an explicit, shared description of *what exists* in a subject area and *how those things relate*.
# MAGIC It gives people and software (including Genie) the same vocabulary. Every ontology has five building blocks:
# MAGIC
# MAGIC | Building block | Meaning | Energy Finance example |
# MAGIC |---|---|---|
# MAGIC | **Entity** | A kind of thing that exists | A *commodity product* (WTI crude oil), a *futures contract* (one contract month), a *trading session* |
# MAGIC | **Attribute** | A property of an entity | Crude oil is quoted in *USD per barrel*; one contract is *1,000 barrels* |
# MAGIC | **Relationship** | How entities connect | A price bar *belongs to* a contract and a session; a contract *is for* a product |
# MAGIC | **Classification** | Controlled categories | Crude oil is a *Crude Oil* product; gasoline and diesel are *Refined Products* |
# MAGIC | **Hierarchy** | Areas of knowledge, nested | *Energy Finance* contains *Market Data* and *Refining Economics* |
# MAGIC
# MAGIC Without an ontology, "gas" could mean natural gas or gasoline, "the price" could be per barrel or per gallon, and
# MAGIC nobody knows which table holds the answer. With one, meaning lives **in the platform**, next to the data.
# MAGIC
# MAGIC ## How each building block maps to Unity Catalog
# MAGIC | Building block | Unity Catalog object | Built in |
# MAGIC |---|---|---|
# MAGIC | Entity + attributes | Dimension tables with a **primary key** and column comments | Step 1 |
# MAGIC | Relationship | **Foreign-key** constraints between tables | Step 2 |
# MAGIC | Classification | **Governed tags** with a fixed list of allowed values | Step 3 |
# MAGIC | Hierarchy | **Domains** and subdomains (Beta) | Step 4 |
# MAGIC | Business metric | A **metric view**: one governed definition of each measure | Step 5 |
# MAGIC
# MAGIC **Prerequisite:** the hourly table, daily table and crack-spread view exist (Notebook 2 or `00_load_synthetic_data`).
# MAGIC Re-run this notebook whenever you rebuild those tables.

# COMMAND ----------

# MAGIC %run ./_workshop_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "demo", "Schema")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")


def fq(name: str) -> str:
    """Fully qualified name in the workshop schema."""
    return f"{CATALOG}.{SCHEMA}.{name}"


HOURLY, DAILY, CRACK = fq(HOURLY_TABLE_NAME), fq(DAILY_TABLE_NAME), fq(CRACK_VIEW_NAME)
DIM_PRODUCT, DIM_CONTRACT, DIM_SESSION = fq("dim_commodity_product"), fq("dim_futures_contract"), fq("dim_trading_session")
METRICS = fq("energy_finance_metrics")

missing = [t for t in (HOURLY, DAILY, CRACK) if not spark.catalog.tableExists(t)]
if missing:
    raise RuntimeError(f"Missing {missing}. Run Notebook 2 or 00_load_synthetic_data first.")

# On a re-run, remove the old relationships first so the entity tables can be rebuilt.
for fact, prefix in ((HOURLY, "hourly"), (DAILY, "daily")):
    for target in ("product", "contract", "session"):
        spark.sql(f"ALTER TABLE {fact} DROP CONSTRAINT IF EXISTS {prefix}_fk_{target}")
    spark.sql(f"ALTER TABLE {fact} DROP CONSTRAINT IF EXISTS {prefix}_pk")
print("Ready to build the Energy Finance ontology in", f"{CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Entities: one table per kind of thing
# MAGIC Each **entity** gets its own table, with a **primary key** that identifies one instance (one product, one contract,
# MAGIC one session) and columns for its **attributes**.
# MAGIC
# MAGIC ### 1a. Commodity product (reference data)
# MAGIC These attributes come from the exchange's contract specifications, not from the price feed. That's typical:
# MAGIC an ontology captures business knowledge that the raw data doesn't carry.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DIM_PRODUCT} (
  product_root     STRING NOT NULL COMMENT 'Futures product code (CL, NG, HO, RB). Primary key.',
  product_name     STRING COMMENT 'Human-readable product name.',
  commodity_group  STRING COMMENT 'Classification: Crude Oil, Refined Products, or Natural Gas.',
  exchange         STRING COMMENT 'Listing exchange (all four trade on NYMEX via CME Globex).',
  price_unit       STRING COMMENT 'Unit in which prices are quoted.',
  contract_size    DOUBLE COMMENT 'Quantity of the commodity in one futures contract.',
  contract_unit    STRING COMMENT 'Unit of contract_size.',
  tick_size        DOUBLE COMMENT 'Minimum price increment.',
  CONSTRAINT dim_commodity_product_pk PRIMARY KEY (product_root)
) COMMENT 'Entity: commodity product. One row per futures product, with contract specifications.'
""")
spark.sql(f"""
INSERT INTO {DIM_PRODUCT} VALUES
  ('CL', 'WTI Crude Oil',                'Crude Oil',        'NYMEX', 'USD per barrel', 1000,  'barrels', 0.01),
  ('NG', 'Henry Hub Natural Gas',        'Natural Gas',      'NYMEX', 'USD per MMBtu',  10000, 'MMBtu',   0.001),
  ('HO', 'NY Harbor ULSD (Heating Oil)', 'Refined Products', 'NYMEX', 'USD per gallon', 42000, 'gallons', 0.0001),
  ('RB', 'RBOB Gasoline',                'Refined Products', 'NYMEX', 'USD per gallon', 42000, 'gallons', 0.0001)
""")
display(spark.table(DIM_PRODUCT))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. Futures contract and trading session (derived from the data)
# MAGIC A **futures contract** is one delivery month of a product; the continuous symbol `CL.c.0` points at a different
# MAGIC contract after each roll. A **trading session** is one CME trade date (6 pm to 5 pm New York time).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DIM_CONTRACT} (
  instrument_id      BIGINT NOT NULL COMMENT 'Databento instrument ID of one contract month. Primary key.',
  product_root       STRING COMMENT 'Product this contract is for (references dim_commodity_product).',
  symbol             STRING COMMENT 'Continuous symbol that pointed at this contract, e.g. CL.c.0.',
  contract_sequence  INT    COMMENT '1 = earliest contract month in the data for this product, 2 = next, ...',
  first_trade_date   DATE   COMMENT 'First CME trade date this contract was the front month in the data.',
  last_trade_date    DATE   COMMENT 'Last CME trade date this contract was the front month in the data.',
  CONSTRAINT dim_futures_contract_pk PRIMARY KEY (instrument_id)
) COMMENT 'Entity: futures contract (one delivery month). Derived from the hourly bars.'
""")
spark.sql(f"""
INSERT INTO {DIM_CONTRACT}
SELECT instrument_id, product_root, symbol,
       CAST(ROW_NUMBER() OVER (PARTITION BY product_root ORDER BY MIN(cme_trade_date)) AS INT),
       MIN(cme_trade_date), MAX(cme_trade_date)
FROM {HOURLY}
GROUP BY instrument_id, product_root, symbol
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {DIM_SESSION} (
  cme_trade_date       DATE NOT NULL COMMENT 'CME trade date. Primary key.',
  day_of_week          STRING    COMMENT 'Weekday name.',
  session_open_et      TIMESTAMP COMMENT 'Session open: 6 pm New York time on the prior calendar day.',
  session_close_et     TIMESTAMP COMMENT 'Session close: 5 pm New York time on the trade date.',
  is_complete_session  BOOLEAN   COMMENT 'True if every product has data through the 5 pm close.',
  CONSTRAINT dim_trading_session_pk PRIMARY KEY (cme_trade_date)
) COMMENT 'Entity: CME trading session (trade date).'
""")
spark.sql(f"""
INSERT INTO {DIM_SESSION}
SELECT cme_trade_date,
       DATE_FORMAT(cme_trade_date, 'EEEE'),
       TO_TIMESTAMP(CONCAT(CAST(DATE_SUB(cme_trade_date, 1) AS STRING), ' 18:00:00')),
       TO_TIMESTAMP(CONCAT(CAST(cme_trade_date AS STRING), ' 17:00:00')),
       BOOL_AND(is_complete_session)
FROM {DAILY}
GROUP BY cme_trade_date
""")
display(spark.table(DIM_CONTRACT).orderBy("product_root", "contract_sequence"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Relationships: primary and foreign keys
# MAGIC The price tables are **facts**: measurements about entities. A **foreign key** records "this column points at that
# MAGIC entity". In Databricks these constraints are *informational*: they aren't enforced on write, but they document the
# MAGIC model for people and tools, and Genie uses them to understand how to join tables.
# MAGIC
# MAGIC ```
# MAGIC dim_commodity_product (product_root) ◄── dim_futures_contract.product_root
# MAGIC         ▲                                        ▲
# MAGIC         │ product_root                           │ instrument_id
# MAGIC   hourly bars / daily table ─────────────────────┘
# MAGIC         │ cme_trade_date
# MAGIC         ▼
# MAGIC dim_trading_session (cme_trade_date)
# MAGIC ```

# COMMAND ----------

RELATIONSHIPS = []
for fact, prefix, key_cols in ((HOURLY, "hourly", ("symbol", "bar_start_utc")),
                               (DAILY, "daily", ("symbol", "cme_trade_date"))):
    for col in key_cols:
        spark.sql(f"ALTER TABLE {fact} ALTER COLUMN {col} SET NOT NULL")
    spark.sql(f"ALTER TABLE {fact} ADD CONSTRAINT {prefix}_pk PRIMARY KEY ({', '.join(key_cols)})")
    for target, col, dim in (("product", "product_root", DIM_PRODUCT),
                             ("contract", "instrument_id", DIM_CONTRACT),
                             ("session", "cme_trade_date", DIM_SESSION)):
        spark.sql(f"ALTER TABLE {fact} ADD CONSTRAINT {prefix}_fk_{target} FOREIGN KEY ({col}) REFERENCES {dim} ({col})")
        RELATIONSHIPS.append((fact, col, dim))
spark.sql(f"ALTER TABLE {DIM_CONTRACT} DROP CONSTRAINT IF EXISTS contract_fk_product")
spark.sql(f"ALTER TABLE {DIM_CONTRACT} ADD CONSTRAINT contract_fk_product FOREIGN KEY (product_root) "
          f"REFERENCES {DIM_PRODUCT} (product_root)")
RELATIONSHIPS.append((DIM_CONTRACT, "product_root", DIM_PRODUCT))

# Because the constraints aren't enforced, check them: every reference must find its entity.
for table, col, dim in RELATIONSHIPS:
    orphans = spark.sql(f"SELECT COUNT(*) AS n FROM {table} t LEFT ANTI JOIN {dim} d USING ({col})").first()["n"]
    status = "OK" if orphans == 0 else f"{orphans} ORPHANS"
    print(f"{table.split('.')[-1]:38} {col:15} -> {dim.split('.')[-1]:24} {status}")
    if orphans:
        raise ValueError(f"{orphans} rows in {table} have no matching {dim}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Classification: governed tags
# MAGIC A **governed tag** is a tag key with a fixed list of allowed values, managed centrally. It's the ontology's
# MAGIC controlled vocabulary: nobody can invent a new category by typo.
# MAGIC
# MAGIC | Governed tag | Applied to | Allowed values |
# MAGIC |---|---|---|
# MAGIC | `ontology_type` | tables and views | `entity`, `fact`, `derived`, `metric` |
# MAGIC | `semantic_type` | columns | `identifier`, `price`, `volume`, `timestamp`, `flag` |

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

w = WorkspaceClient()


def ensure_governed_tag(tag_key: str, description: str, values: list[str] | None = None) -> None:
    """Create a governed tag (tag policy) unless it already exists."""
    body = {"tag_key": tag_key, "description": description}
    if values:
        body["values"] = [{"name": v} for v in values]
    try:
        w.api_client.do("POST", "/api/2.1/tag-policies", body=body)
        print(f"created governed tag  {tag_key}")
    except DatabricksError as exc:
        if "ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc).lower():
            raise
        print(f"governed tag exists   {tag_key}")


ensure_governed_tag("ontology_type", "Role of a table in the Energy Finance ontology.",
                    ["entity", "fact", "derived", "metric"])
ensure_governed_tag("semantic_type", "Meaning of a column in the Energy Finance ontology.",
                    ["identifier", "price", "volume", "timestamp", "flag"])

for table in (DIM_PRODUCT, DIM_CONTRACT, DIM_SESSION):
    spark.sql(f"ALTER TABLE {table} SET TAGS ('ontology_type' = 'entity')")
for table in (HOURLY, DAILY):
    spark.sql(f"ALTER TABLE {table} SET TAGS ('ontology_type' = 'fact')")
spark.sql(f"ALTER VIEW {CRACK} SET TAGS ('ontology_type' = 'derived')")

COLUMN_SEMANTICS = {
    "identifier": ["symbol", "product_root", "instrument_id", "cme_trade_date"],
    "price": ["open", "high", "low", "close"],
    "volume": ["volume"],
    "timestamp": ["bar_start_utc", "bar_start_et"],
    "flag": ["is_contract_roll", "is_complete_session"],
}
for table in (HOURLY, DAILY):
    existing = {f.name for f in spark.table(table).schema.fields}
    for semantic, columns in COLUMN_SEMANTICS.items():
        for col in columns:
            if col in existing:
                spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} SET TAGS ('semantic_type' = '{semantic}')")
print("Classification tags applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Try it:** governed tags reject values that aren't on the list. Uncomment and run the next cell; you should get
# MAGIC `UC_TAG_POLICY_VALUE_NOT_ALLOWED`.

# COMMAND ----------

# spark.sql(f"ALTER TABLE {DIM_PRODUCT} SET TAGS ('ontology_type' = 'spreadsheet')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Hierarchy: the Energy Finance domain
# MAGIC A **domain** groups data assets by area of knowledge so people can find them, and it can contain **subdomains**.
# MAGIC Each domain is backed by its own governed tag; tagging a table with it places the table in the domain.
# MAGIC A subdomain's tag key must follow the pattern `<parent tag>/<SubdomainName>`.
# MAGIC
# MAGIC ```
# MAGIC Energy Finance                 (tag: energy_finance)            → dim_commodity_product
# MAGIC ├── Market Data                (tag: energy_finance/MarketData) → hourly, daily, contracts, sessions, metrics
# MAGIC └── Refining Economics         (tag: energy_finance/RefiningEconomics) → crack-spread view
# MAGIC ```
# MAGIC Domains are a **Beta** feature, so the API may change.

# COMMAND ----------

DOMAINS = [
    # (domain_id, tag_key, parent_domain_id, subtitle, description)
    ("energy-finance", "energy_finance", None, "Energy commodity markets and margins",
     "Prices, volumes and derived economics for CME energy futures: WTI crude oil, Henry Hub natural gas, "
     "RBOB gasoline and NY Harbor ULSD. Built in the UCalgary DSML Genie workshop."),
    ("energy-finance-market-data", "energy_finance/MarketData", "energy-finance", "Futures prices and volumes",
     "Hourly and daily OHLCV bars, futures contracts, trading sessions and market metrics."),
    ("energy-finance-refining-economics", "energy_finance/RefiningEconomics", "energy-finance",
     "Refinery margin indicators", "Crack spreads: the margin between crude oil and the products refined from it."),
]


def ensure_domain(domain_id: str, tag_key: str, parent: str | None, subtitle: str, description: str) -> None:
    """Create a domain (and its governed tag) unless it already exists."""
    ensure_governed_tag(tag_key, f"Domain tag for {domain_id}.")
    body = {"tag_key": tag_key, "subtitle": subtitle, "description": description}
    if parent:
        body["parent_domain_id"] = parent
    try:
        w.api_client.do("POST", "/api/2.0/domains", query={"domain_id": domain_id}, body=body)
        print(f"created domain        {domain_id}")
    except DatabricksError as exc:
        if "ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc).lower():
            raise
        print(f"domain exists         {domain_id}")


for spec in DOMAINS:
    ensure_domain(*spec)

ASSIGNMENTS = {
    "energy_finance": [DIM_PRODUCT],
    "energy_finance/MarketData": [HOURLY, DAILY, DIM_CONTRACT, DIM_SESSION],
    "energy_finance/RefiningEconomics": [CRACK],
}
for tag_key, tables in ASSIGNMENTS.items():
    for table in tables:
        kind = "VIEW" if table == CRACK else "TABLE"
        spark.sql(f"ALTER {kind} {table} SET TAGS ('{tag_key}')")
print("Assets placed in domains.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Business metrics: a metric view
# MAGIC Notebook 2 computed annualized volatility with a formula in a SQL cell. If every analyst writes that formula
# MAGIC themselves, they'll eventually disagree. A **metric view** defines each measure **once**, joined to the entities, so
# MAGIC SQL, dashboards and Genie all use the same definition. Query it with `MEASURE()`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {METRICS}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Energy Finance market metrics: governed definitions over the daily table, complete sessions only."
source: {DAILY}
filter: is_complete_session
joins:
  - name: dim_product
    source: {DIM_PRODUCT}
    on: source.product_root = dim_product.product_root
dimensions:
  - name: Trade Date
    expr: cme_trade_date
    comment: "CME trade date"
  - name: Product
    expr: dim_product.product_name
    comment: "Commodity product name"
  - name: Commodity Group
    expr: dim_product.commodity_group
    comment: "Crude Oil, Refined Products, or Natural Gas"
  - name: Day of Week
    expr: day_of_week
measures:
  - name: Trade Days
    expr: COUNT(DISTINCT cme_trade_date)
  - name: Average Close
    expr: AVG(close)
    comment: "Average daily close in the product's price unit; compare only within one product"
  - name: Annualized Volatility Pct
    expr: STDDEV_SAMP(daily_return_pct) * SQRT(252)
    comment: "Standard deviation of daily returns x sqrt(252); roll days are excluded because their return is null"
  - name: Average Daily Range Pct
    expr: AVG(daily_range_pct)
    comment: "Average (high - low) / open, in percent"
  - name: Total Volume
    expr: SUM(volume)
    comment: "Contracts traded"
$$
""")
spark.sql(f"ALTER VIEW {METRICS} SET TAGS ('ontology_type' = 'metric')")
spark.sql(f"ALTER VIEW {METRICS} SET TAGS ('energy_finance/MarketData')")  # key-only tags go in their own statement

display(spark.sql(f"""
    SELECT `Commodity Group`, `Product`,
           ROUND(MEASURE(`Annualized Volatility Pct`), 1) AS annualized_volatility_pct,
           ROUND(MEASURE(`Average Daily Range Pct`), 2)   AS avg_daily_range_pct,
           MEASURE(`Trade Days`)                          AS trade_days
    FROM {METRICS}
    GROUP BY ALL
    ORDER BY annualized_volatility_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Explore the ontology you built
# MAGIC Everything above is now **metadata in Unity Catalog**. You can query it like data, and browse it in Catalog
# MAGIC Explorer (open any table to see its tags, constraints and lineage) and in the Domains view (Beta).

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, tag_name, tag_value
    FROM {CATALOG}.information_schema.table_tags
    WHERE schema_name = '{SCHEMA}' AND (tag_name = 'ontology_type' OR tag_name LIKE 'energy_finance%')
    ORDER BY tag_name, table_name
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT tc.table_name, tc.constraint_type, tc.constraint_name, kcu.column_name
    FROM {CATALOG}.information_schema.table_constraints tc
    JOIN {CATALOG}.information_schema.key_column_usage kcu
      ON tc.constraint_catalog = kcu.constraint_catalog AND tc.constraint_schema = kcu.constraint_schema
     AND tc.constraint_name = kcu.constraint_name
    WHERE tc.table_schema = '{SCHEMA}'
    ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name
"""))

# COMMAND ----------

domains = w.api_client.do("GET", "/api/2.0/domains")
by_id = {d["domain_id"]: d for d in (domains if isinstance(domains, list) else domains.get("domains", []))}
for domain_id, _, parent, _, _ in DOMAINS:
    d = by_id.get(domain_id, {})
    indent = "    └── " if parent else ""
    print(f"{indent}{domain_id}  (tag: {d.get('tag_key')})  {d.get('subtitle', '')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion
# MAGIC 1. Which facts in the ontology came from the **data**, and which came from **business knowledge** (contract
# MAGIC    specifications, classifications)? Why does Genie need both?
# MAGIC 2. Where would a *Brent crude* product fit? What would you add to each step?
# MAGIC 3. The foreign keys aren't enforced. What could go wrong, and how did Step 2 guard against it?
# MAGIC
# MAGIC **Next:** run `04_create_genie_space`. It adds the entity tables and metric view to the Genie space when they exist.
