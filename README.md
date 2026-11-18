# From Market Data API to Genie: Energy Futures Analytics on Databricks

Workshop materials for the University of Calgary DSML Genie workshop (Fall 2026).

> **Disclaimer:** The dataset provided in [`data/synthetic/`](data/synthetic/) is synthetic. It was generated for
> instructional purposes only and does not represent actual market prices, volumes, or trading activity. To run this
> exercise with live market data, each participant must register for their own [Databento](https://databento.com)
> account and generate a personal API key. Market data obtained through Databento is subject to Databento's terms of
> service and the licensing terms of the originating exchange, and may not be redistributed.

You'll pull real **CME energy futures** data from the [Databento](https://databento.com) API into
**Unity Catalog**, explore it with basic statistics, and then ask questions about it in plain English with
**Databricks Genie**.

| Product | Code | Quoted in |
|---|---|---|
| WTI crude oil | `CL` | USD per barrel |
| Henry Hub natural gas | `NG` | USD per MMBtu |
| RBOB gasoline | `RB` | USD per gallon |
| NY Harbor ULSD (heating oil / diesel) | `HO` | USD per gallon |

## Why this workshop
Genie answers questions by writing SQL against **governed tables**. It can't call an API itself. So the real
work behind a good Genie experience is data engineering: landing the data, modelling it so it reflects how the
market actually works, and giving Genie the business context it needs. This workshop walks that whole path.

## What you need
- A **Databricks Free Edition** workspace (free sign-up at <https://www.databricks.com/learn/free-edition>).
- **Track A only:** your own Databento account and personal API key (register at <https://databento.com>).
- **Track A only:** the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html), to store your key
  as a Unity Catalog secret.

## Two ways through the workshop
| Track | Run | When to use |
|---|---|---|
| **A: Live API** | `01` → `02` → `03` → `04` | You have a Databento API key and want to build the pipeline yourself |
| **B: Synthetic data** | `00` → `03` → `04` | No Databento account needed: load the synthetic dataset in `data/synthetic/` and go straight to Genie |

| Notebook | What it does |
|---|---|
| `01_databento_discovery` | Explore the API (datasets, schemas, symbology), then pull 10 rows into a table |
| `02_databento_30day_ingest` | Pull 30 days of hourly bars and build an hourly table, a daily table, and a crack-spread view, plus statistics: volatility, correlation, volume by hour, refining margin |
| `03_energy_finance_domain` | Model the **Energy Finance ontology**: entity tables with primary keys, foreign-key relationships, governed tags, a domain hierarchy, and a metric view |
| `04_create_genie_space` | Create a Genie space **as code**: tables, instructions, trusted example SQL, and sample questions |
| `00_load_synthetic_data` | Build the same tables from the synthetic CSVs in this repo (no API key needed) |
| `_workshop_common` | Shared helpers (table comments, product data, crack-spread view) that the others load with `%run` |

## Setup

### 1. Get the notebooks into your workspace
In Databricks: **Workspace → Create → Git folder**, and paste this repository's URL:
`https://github.com/harrison-chen87/ucalgary-dsml-genie-f2026`.
The notebooks are in `notebooks/`.

### 2. Create a schema for the workshop
Run this in a notebook or the SQL editor:
```sql
CREATE SCHEMA IF NOT EXISTS workspace.demo;
```

### 3. (Track A) Store your Databento API key as a Unity Catalog secret
Never paste an API key into a notebook. Store it as a **Unity Catalog secret**, a governed object at
`catalog.schema.name` with access controlled by grants, just like a table.

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com -p my-free-edition
read -rs DATABENTO_API_KEY      # paste your key and press Enter; nothing is shown on screen
databricks secrets-uc create-secret databento_api_key workspace demo "$DATABENTO_API_KEY" -p my-free-edition
unset DATABENTO_API_KEY
```
`read -rs` keeps the key off your screen and out of your shell history. The notebooks then read it like this:
```python
DATABENTO_API_KEY = dbutils.secrets.get(catalog="workspace", schema="demo", key="databento_api_key")
```

### 4. Run the notebooks
Open each notebook and click **Run all**. Each one pins **serverless environment version 6** in its header,
which the UC-secret form of `dbutils.secrets.get` needs.

## The data-modelling ideas you'll meet
- **Trade date isn't calendar date.** CME energy trades from 6 pm to 5 pm New York time, and a session that opens
  Sunday evening belongs to **Monday**. Bucketing by UTC day creates fake "Sunday" rows.
- **Contract rolls.** `CL.c.0` means "the front-month contract", which changes every month. The price jump at a roll
  isn't a real return, so it's flagged (`is_contract_roll`) and excluded from return statistics.
- **Incomplete sessions.** Today's session isn't over yet (`is_complete_session = false`), so it's excluded from
  statistics.
- **The 3-2-1 crack spread.** A crack spread is the price difference between crude oil and the refined products made
  from it; it's the key benchmark for refinery margins. Refiners turn 3 barrels of crude into about 2 of gasoline and 1 of diesel. The margin,
  in USD per barrel, is `(2 × RB × 42 + HO × 42 − 3 × CL) / 3`. There are 42 gallons in a barrel.

## The ontology: giving data shared meaning
An **ontology** describes what exists in a subject area and how things relate. Notebook 3 builds one for Energy
Finance and maps each building block to Unity Catalog:

| Building block | Example | Unity Catalog object |
|---|---|---|
| Entity | commodity product, futures contract, trading session | dimension table with a primary key |
| Relationship | a price bar belongs to a contract and a session | foreign-key constraint |
| Classification | crude oil vs refined products; entity vs fact | governed tag with allowed values |
| Hierarchy | Energy Finance → Market Data, Refining Economics | domain and subdomains (Beta) |
| Business metric | annualized volatility, defined once | metric view |

## The Genie exercise
After Notebook 4, open the Genie space it links to and try:
1. *"What is the average price of gas?"* Genie should choose **natural gas**, not gasoline, because of the
   synonym rules in its instructions.
2. *"Which product is the most volatile?"* Compare the answer with section 7.1 of Notebook 2. It should match,
   because Genie reuses the trusted volatility SQL.
3. Ask something nobody anticipated, then click **Show code** and decide whether the SQL is right. That habit is
   the most important skill for working with Genie.

## Troubleshooting
| Error | Fix |
|---|---|
| `TypeError: ... unexpected keyword argument 'catalog'` | The environment pin is missing or misplaced. The `# /// script` block must come right **after** `# Databricks notebook source`. |
| `422 data_schema_not_fully_available` | You asked for data past the schema's last published time. The notebooks clamp the window automatically; check any edits. |
| `UNSUPPORTED_ARROWTYPE Int(8, false)` | Databento uses unsigned integers. Cast them to `int64` before Spark or `display()` sees them. |
| `Missing tables ...` in Notebook 3 or 4 | Run Notebook 2 (Track A) or `00_load_synthetic_data` (Track B) first. |

## A note on the data
- **Track A** pulls real CME Group data through Databento under **your own Databento account**. Please don't publish
  data you pull with your key.
- **Track B** uses the **synthetic** dataset in [`data/synthetic/`](data/synthetic/); see the disclaimer above.
