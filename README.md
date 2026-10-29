# Stock Market Data Pipeline on Databricks (Incremental Loads + SCD Type 2)

A daily pipeline that pulls stock prices from a public API, loads only the new days, and keeps a full history of how each company's attributes change over time.

**Status:** work in progress. I'm aiming to have it complete and tested by 10 Nov 2026. I'll only add numbers to this README after I've actually measured them.

## Why I built this

I'm a data engineer, and I'm interested in financial markets (I hold the NISM Series-8 Equity Derivatives certification). I wanted a project that works the way real data teams handle market data, not just a notebook that reads one CSV.

Two problems kept coming up when I read about financial reporting:

1. **Reloading everything every day is slow and risky.** If a job fails and someone re-runs it, prices can get counted twice.
2. **Overwriting loses history.** If a company's classification changes, a simple overwrite means an old report now shows today's values instead of the values that were true back then.

So this project is my attempt to solve both of those properly.

## What the pipeline has to do

1. Load daily prices incrementally, so only new trading days get processed.
2. Be safe to re-run. Running the same day twice should never create duplicates.
3. Keep the full history of company attributes using SCD Type 2.
4. Work within free-tier limits: the API allows 25 requests a day, and I'm using Databricks Free Edition.

## What I've done so far

| Step | What I did | Status |
|---|---|---|
| Extract | Python script that calls the Alpha Vantage API for 8 tickers. It retries network errors, catches API errors that come back inside a normal "200 OK" reply, waits between calls to stay under the rate limit, turns the date-keyed response into one row per day, and saves one JSON Lines file per ticker per day. Re-running a day overwrites that day's file, so there are no duplicates. | Done |
| Change of plan | I originally planned to get sector and industry from the API's company-overview endpoint, but it turned out to be premium on the free tier. Instead of dropping SCD Type 2, I changed the design: name, sector and industry come from a reference file I maintain (`companies.csv`), and I calculate a real, changing attribute from the prices, `trend_status` (whether the close is above or below its 20-day moving average). | Done |
| Bronze layer | Auto Loader picks up only new files and adds `load_ts` and `source_file` to every row. | Done |
| Silver layer | Convert types, remove duplicates per (ticker, trade_date), and count values that fail to convert. | Done |
| Fact table | `fact_daily_price`: only loads dates after the last loaded date, using MERGE on (ticker, trade_date). | In progress |
| Dimension table | `dim_company` with SCD Type 2. A MERGE closes current rows whose values changed, then an anti-join inserts the new current rows. I've already tested this logic on sample data. | Planned |
| Tests | Re-run safety, a planned change in `companies.csv`, and row counts that match between layers. | Planned |
| Scheduling | A Databricks Job that runs every step in order each day. | Planned |

## Results

I'll add the real numbers here once the full pipeline has run end to end: days loaded, row counts, the re-run test, how many real `trend_status` changes were captured, and run time.

## How it works

```
Alpha Vantage API (TIME_SERIES_DAILY)
      |  extract/extract.py (runs on my laptop)
      v
landing/prices_<TICKER>_<YYYY-MM-DD>.jsonl   +   reference/companies.csv
      |  uploaded to a Unity Catalog Volume
      v
Bronze   Auto Loader -> bronze_prices (+ load_ts, source_file), bronze_companies
      v
Silver   typed, deduplicated prices
      v
Gold     fact_daily_price  (incremental + MERGE)
         dim_company       (SCD Type 2 on sector, industry, trend_status)
      v
Tests    re-run safety, SCD2 history, row-count checks
```

A few decisions I made along the way:

- **Flatten in the extract, convert types in silver.** Bronze keeps the raw values, so if something goes wrong I can reprocess without calling the API again (which matters with only 25 calls a day).
- **Every step is safe to re-run:** one file per ticker per day, the Auto Loader checkpoint, MERGE on business keys, and SCD2 steps that change nothing the second time.
- **SCD Type 2 in two steps instead of one.** It's easier to read and debug. The downside is that if the job fails between the two steps, a company can be left without a current row until the next run, which fixes it.
- **I compare values with `<=>` (NULL-safe equals) in the SCD2 MERGE**, because a normal `<>` returns UNKNOWN when a value is NULL and would miss the change.

## Data

| Source | What it gives | Notes |
|---|---|---|
| Alpha Vantage `TIME_SERIES_DAILY` | Daily open, high, low, close, volume | Free tier: 25 requests a day, 5 a minute. 8 tickers = 8 calls a day |
| `reference/companies.csv` | Name, sector, industry | I maintain this by hand and change it on purpose to test SCD Type 2 |
| `trend_status` (calculated) | `above_avg` or `below_avg` vs the 20-day moving average | Changes with real prices, so the SCD2 history is real |

Tickers: AAPL, AMD, AMZN, GOOGL, IBM, MSFT, NVDA, TSLA

## How to run the extract

1. Create a `.env` file in the project folder with one line: `API_Key=your_key_here`. It's in `.gitignore`, so it never gets committed.
2. Install the packages: `pip install requests python-dotenv`
3. Run: `python extract/extract.py`
4. The files appear in `landing/`. Upload them to the Databricks Volume.

The Databricks steps will be added here when the notebooks are in the repo.

## Tests

| Test | What it proves | Status |
|---|---|---|
| Run the same day twice | No duplicate rows anywhere | Not run yet |
| Load day 1, then day 2 | Only the new rows are processed | Not run yet |
| Change one company's industry in `companies.csv` | Old row closed, new row current, exactly one current row per ticker | Not run yet |
| Row counts | Bronze, silver and gold counts match up | Not run yet |

## Limitations

- The free API limits how many tickers I can track.
- Name, sector and industry are maintained by hand, not pulled live.
- Databricks Free Edition is serverless only and has fair-usage limits.

## Tech stack

Python (requests, python-dotenv), Databricks Free Edition, PySpark, Delta Lake (MERGE, schema enforcement), Auto Loader, Unity Catalog Volumes, Databricks Jobs, Git/GitHub

## Repository layout

```
extract/      extract.py (done)
reference/    companies.csv
notebooks/    bronze, silver, gold (in progress)
tests/        re-run, incremental, SCD2, row-count checks
README.md
```
