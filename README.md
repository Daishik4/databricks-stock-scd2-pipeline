# Stock Market Data Pipeline on Databricks — Incremental Loads + SCD Type 2

> **Status: work in progress.** Target: complete and tested by 10 Nov 2026.
> Every number in the Results section will come from real runs — nothing is filled in before it is measured.

## Problem

Financial teams need two things from market data:
1. **Daily prices loaded incrementally** — only new days are processed, and a re-run never creates duplicates.
2. **History of company attributes** (sector, industry, market-cap band) — so a report for a past date uses the attributes that were true *on that date*, not today's.

This project builds a small but realistic pipeline that does both on Databricks.

## Architecture

```
Alpha Vantage API
      │  (Python extract: retries, rate-limit pauses, one JSON file per ticker per day)
      ▼
Unity Catalog Volume  /Volumes/<catalog>/<schema>/raw/          ← landing zone
      │  (Auto Loader: picks up only files it has not seen before)
      ▼
Bronze  (Delta)  raw records + load_date + source_file
      ▼
Silver  (Delta)  correct types, duplicates removed (latest record per key)
      ▼
Gold    dim_company        — SCD Type 2 (sector, industry, market_cap_band) via Delta MERGE
        fact_daily_price   — incremental by trade date (watermark)
      ▼
Tests   re-run safety · SCD2 history · row-count reconciliation
Schedule: Databricks Job (daily)
```

## Data source

- **Alpha Vantage** free API: daily prices and company overview (sector, industry, market capitalisation).
- **Free-tier limits:** about 25 requests per day and 5 per minute → the pipeline tracks **[FILL: number] tickers**: **[FILL: list of tickers]**.
- **Honest note:** real company attributes change rarely. To prove the SCD Type 2 logic, the tests also use a small **test dataset with planned changes** (for example, a company changing sector on day 3). Real-data changes are reported separately in Results.

## How it works

### 1. Extract (Python)
- Calls the API for each ticker, waits between calls to stay under the rate limit, retries on temporary errors.
- Writes one file per ticker per day: `prices_<TICKER>_<YYYY-MM-DD>.json`.
- **Re-running the same day overwrites the same file**, so the landing zone never gets duplicates.
- The API key is read from a secret / environment variable — **it is never stored in this repo.**

### 2. Bronze — Auto Loader
- Auto Loader reads only **new** files from the landing zone and remembers what it has already processed (checkpoint).
- Adds `load_date` and `source_file` to every row for traceability.

### 3. Silver — clean and deduplicate
- Casts types (dates, decimals).
- Keeps the latest record per key using `row_number()` with a tie-breaker.

### 4. Gold — dim_company (SCD Type 2)
- Tracked columns: `sector`, `industry`, `market_cap_band`.
- When a tracked value changes: the current row is **closed** (`valid_to`, `is_current = false`) and a **new row is inserted** (`valid_from`, `is_current = true`).
- Implemented with **Delta Lake MERGE**.

### 5. Gold — fact_daily_price (incremental)
- Loads only trade dates **after the last loaded date** (watermark).
- MERGE on `(ticker, trade_date)` so a re-run updates instead of duplicating.

## How to run

1. **[FILL]** Create the catalog, schema and volume.
2. **[FILL]** Store the API key as a secret / environment variable.
3. **[FILL]** Run `extract` → `bronze` → `silver` → `gold` notebooks, or run the Databricks Job.

## Tests

| Test | What it proves | Status |
|---|---|---|
| Run the same day twice | No duplicate rows anywhere | [FILL] |
| Load day 1, then day 2 | Only day 2's new rows are processed | [FILL] |
| Planned sector change on day 3 | Old row closed, new row current, exactly one current row per company | [FILL] |
| Row counts | Bronze → Silver → Gold counts reconcile | [FILL] |

## Results

[FILL after real runs: rows loaded, number of days, run time, SCD2 changes found in real data.]

## Limitations

- Free-tier API limits restrict the number of tickers.
- Databricks Free Edition: serverless only, fair-usage limits, restricted outbound internet.
- Real attribute changes are rare; SCD2 is proven mainly with planned test changes.

## Tech stack

Python · Databricks (Free Edition) · PySpark · Delta Lake (MERGE) · Auto Loader · Unity Catalog Volumes · Databricks Jobs

## Repository layout (planned)

```
extract/        Python API extract
notebooks/      bronze, silver, gold
tests/          re-run, incremental, SCD2, reconciliation tests
test_data/      planned-change dataset for SCD2 tests
README.md
```
