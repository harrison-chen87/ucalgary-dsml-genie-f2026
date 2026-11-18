# Synthetic CME energy futures dataset

> **Disclaimer:** This dataset is synthetic. It was generated for instructional purposes only and does not
> represent actual market prices, volumes, or trading activity. To run this exercise with live market data, each
> participant must register for their own [Databento](https://databento.com) account and generate a personal API key.
> Market data obtained through Databento is subject to Databento's terms of service and the licensing terms of the
> originating exchange, and may not be redistributed.

| File | Rows | Grain |
|---|---|---|
| `energy_futures_hourly.csv` | 5,388 | one row per product per traded hour |
| `energy_futures_daily.csv` | 240 | one row per product per CME trade date |

**Products:** WTI crude oil (`CL.c.0`), Henry Hub natural gas (`NG.c.0`), NY Harbor ULSD / heating oil (`HO.c.0`),
RBOB gasoline (`RB.c.0`). **Trade dates:** 2026-08-24 to 2026-11-16 (60 dates).

**How it was made:** hourly bars were simulated from a statistical model calibrated to real CME energy futures
(volatility, correlation between products, volume by hour, daily range, contract-roll gaps). The data follows the
market's conventions: 6 pm to 5 pm ET sessions, CME holidays, the contract-roll calendar, and tick sizes. Every row
has `source_dataset = SYNTHETIC`.

**Limitations:** prices follow a random walk, so levels and spreads (such as the crack spread) drift away from the
real market. News events and volatility clustering are not modelled.

Load it with `notebooks/00_load_synthetic_data`. Column descriptions are added as table comments when it's loaded.
