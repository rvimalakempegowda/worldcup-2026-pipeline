# World Cup 2026 Live Analytics Pipeline

[![CI](https://github.com/rvimalakempegowda/worldcup-2026-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/rvimalakempegowda/worldcup-2026-pipeline/actions/workflows/ci.yml)
[![Airflow DAG validation](https://github.com/rvimalakempegowda/worldcup-2026-pipeline/actions/workflows/airflow-dag-validation.yml/badge.svg)](https://github.com/rvimalakempegowda/worldcup-2026-pipeline/actions/workflows/airflow-dag-validation.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

End-to-end data engineering pipeline that ingests FIFA World Cup 2026 match
data, processes it through a medallion architecture (bronze/silver/gold) on
Delta Lake, and serves a live dashboard for tracking groups, standings, and
match events.

Built to mirror a production Databricks deployment: the same PySpark/Delta
code in this repo is designed to run unmodified on Databricks (ADLS Gen2 +
Unity Catalog), with Airflow handling orchestration and GitHub Actions
handling CI/CD.
handling CI/CD.

## Architecture

```
Scraper / API client  -->  Bronze (raw)  -->  Silver (cleaned, SCD2)  -->  Gold (aggregates)  -->  Dashboard
        |
   Airflow DAG (orchestration, retries, scheduling)
```

- **Ingestion** (`ingestion/`): pulls match, team, group, and stadium data.
  Two interchangeable sources behind one interface (`SourceClient`):
  - `WikipediaScraper` — scrapes the public 2026 World Cup results tables.
    This is the real, production scraper.
  - `FixtureSource` — replays a saved snapshot of the same shape of data.
    Used for local development, CI tests, and any environment where
    outbound scraping isn't available (e.g. a locked-down sandbox or a
    network with scraping blocked). Swapping back to the live scraper is a
    one-line change (`SOURCE=wikipedia` instead of `SOURCE=fixture`).
  - This pattern -- abstracting the source behind an interface so the
    rest of the pipeline doesn't care where bytes came from -- is the same
    pattern used for swapping a community API for a paid one (Sportmonks,
    TheStatsAPI) without touching downstream code.

- **Bronze** (`bronze/`): lands raw scraped/ingested records as-is into
  Delta tables, partitioned by ingestion date. Append-only, schema-on-read.

- **Silver** (`silver/`): cleans and conforms bronze data into typed Delta
  tables. Group standings use **SCD Type 2** (`effective_from`,
  `effective_to`, `is_current`) so you can query "what did the table look
  like after matchday 2" -- the same pattern used for taxi zone history in
  the streaming project.

- **Gold** (`gold/`): aggregated, BI-ready tables: group standings, top
  scorers, team form, goals-per-matchday trend.

- **Live event simulator** (`ingestion/live_event_simulator.py`): generates
  a realistic stream of match events (goals, cards, substitutions) against
  real teams/fixtures, used to drive the live dashboard demo deterministically
  in an interview setting, independent of whether a real match is in progress.

- **Dashboard** (`dashboard/`): Streamlit app reading from the Gold layer.

- **Orchestration** (`airflow_dags/`): two Airflow 3.x TaskFlow DAGs.
  - `worldcup_live_pipeline`: runs every 5 minutes. A `@task.short_circuit`
    gate (`check_for_live_matches`) checks whether any match is in
    progress; if not, every downstream task is skipped and the run exits
    immediately. If a match is live, the run falls through to
    ingest -> bronze -> silver -> gold. This gives the effect of "poll
    often during matches, stay idle otherwise" without needing Airflow to
    dynamically rewrite its own schedule -- which it can't do natively.
  - `worldcup_daily_maintenance`: runs once a day. Refreshes reference
    data, rebuilds gold, and runs a data-quality check task (row-count
    sanity, null-key checks, referential integrity between standings and
    teams) that raises -- and triggers Airflow's retry/alerting -- if any
    check fails.
  - Both DAGs were validated against a real local Airflow 3.2.2 instance
    (`airflow db migrate`, `airflow dags list`, `airflow tasks test`) --
    not just written to look plausible. See "Running the orchestration
    locally" below.
  - On Databricks, swap each `BashOperator` for a
    `DatabricksSubmitRunOperator` (or point Airflow at the Databricks Jobs
    defined in `databricks.yml` via `DatabricksRunNowOperator`) -- DAG
    structure and the short-circuit gate stay identical.

- **CI/CD** (`.github/workflows/`): two GitHub Actions workflows.
  - `ci.yml` runs on every push/PR to `main`: `lint` (ruff + black) and
    `test` (pytest unit tests) run in parallel; `pipeline-smoke-test` runs
    only if both pass, and executes the real bronze -> silver -> gold
    chain against the fixture data source, then asserts each gold table
    has the expected shape (non-empty, no null keys, non-negative points)
    before uploading the gold tables as a build artifact. This is the
    check that actually catches schema/data-shape regressions -- the kind
    of bug that slipped past the unit tests during development of this
    project (an all-null bronze column that Spark couldn't type-infer)
    only surfaced when real bronze-shaped data flowed through the full
    chain, which is exactly what this job re-creates on every PR.
  - `airflow-dag-validation.yml` runs whenever `airflow_dags/**` changes:
    installs Airflow with the official constraints file, imports every DAG
    module directly (catches syntax/import errors immediately), then runs
    `airflow dags reserialize` + `airflow dags list-import-errors` to
    confirm the scheduler itself would accept both DAGs with zero import
    errors.
  - Both workflows were validated by running the exact same command
    sequence locally against a clean environment before being committed --
    see the "Running CI checks locally" section below.

## Why scrape + a fixture fallback, not just an API?

Free community World Cup APIs exist but are unstable: some require
self-hosting (Node + MongoDB), some require auth, some rate-limit
aggressively. A scraper against a stable public source (Wikipedia's
tournament tables) plus a documented fixture fallback for offline
development is a more realistic and more resilient design than depending
on a single third-party API -- and it's a better story in an interview:
it shows you designed for source instability, not just happy-path ingestion.

## Running locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Ingest (defaults to fixture source; set SOURCE=wikipedia for live scrape)
python ingestion/run_ingestion.py

# 2. Build bronze -> silver -> gold
python bronze/load_bronze.py
python silver/transform_silver.py
python gold/build_gold.py

# 3. Run the live event simulator (optional, for dashboard demo)
python ingestion/live_event_simulator.py &

# 4. Launch dashboard
streamlit run dashboard/app.py
```

## Running CI checks locally

```bash
pip install ruff black pytest

ruff check .              # lint
black --check .           # formatting
pytest tests/ -v          # unit tests

# full smoke test, same as the pipeline-smoke-test CI job:
python ingestion/run_ingestion.py
python ingestion/live_event_simulator.py --count 10
python bronze/load_bronze.py
python silver/transform_silver.py
python gold/build_gold.py
```

## Running the orchestration locally

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow_dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"

airflow db migrate              # one-time: create the metadata DB
airflow dags reserialize        # parse and register both DAGs
airflow dags list                # confirm both DAGs are discovered

# Run a single task in isolation (no scheduler needed):
airflow tasks test worldcup_live_pipeline check_for_live_matches 2026-06-24
airflow tasks test worldcup_live_pipeline run_ingestion 2026-06-24
airflow tasks test worldcup_daily_maintenance run_data_quality_checks 2026-06-24

# Or run the full scheduler + webserver to watch DAGs execute on schedule:
airflow standalone
```

`airflow_home/` (the local metadata DB and logs) is gitignored -- it's
local state, not part of the pipeline definition.

## Deploying to Databricks

- Swap `LOCAL_DATA_ROOT` in `config.py` for an ADLS Gen2 path
  (`abfss://...@...dfs.core.windows.net/...`).
- Run `bronze/load_bronze.py`, `silver/transform_silver.py`, and
  `gold/build_gold.py` as Databricks Jobs / Lakeflow Jobs, or package as a
  Databricks Asset Bundle (`databricks.yml` included).
- Register Gold tables in Unity Catalog and point Databricks SQL or
  Power BI / Tableau at them instead of (or alongside) the Streamlit app.
- Replace the local Airflow DAG's `BashOperator` calls with
  `DatabricksSubmitRunOperator`.
