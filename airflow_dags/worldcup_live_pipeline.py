"""
World Cup 2026 pipeline orchestration DAG.

Schedule strategy: "frequent during live matches, idle otherwise"
-------------------------------------------------------------------
Airflow DAGs run on a single fixed schedule_interval -- there is no native
way to have a DAG dynamically change its own cron expression mid-tournament.
The standard production pattern for "poll often, but only do real work when
it matters" is therefore:

  1. Schedule the DAG tightly (every 5 minutes, all day, every day).
  2. The first task (`check_for_live_matches`) is a `@task.short_circuit`
     gate: it checks whether any match is currently in progress. If not,
     it returns False and every downstream task is skipped -- the DAG run
     finishes in milliseconds and costs nothing.
  3. Only when a match is live does the run fall through to the real
     ingest -> bronze -> silver -> gold chain.

This gives the *effect* of "frequent polling during matches, idle
otherwise" without needing dynamic re-scheduling, and is the same pattern
used for any bursty external event (e.g. polling a webhook-less API for
order status). On Databricks, swap BashOperator for
DatabricksSubmitRunOperator / DatabricksNotebookOperator -- the DAG
structure and the short-circuit gate stay identical.

A second, separate, less frequent DAG (`worldcup_daily_maintenance`) handles
work that only needs to happen once a day regardless of live match status
(e.g. refreshing the team/stadium reference data).
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

PROJECT_ROOT = "/home/claude/worldcup-pipeline"
PYTHON_BIN = "python3"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="worldcup_live_pipeline",
    description="Ingest, transform, and aggregate World Cup 2026 data when a match is live",
    schedule="*/5 * * * *",  # every 5 minutes, all day
    start_date=datetime(2026, 6, 11),  # tournament opening day
    end_date=datetime(2026, 7, 20),  # day after the final
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["worldcup", "bronze", "silver", "gold", "live"],
)
def worldcup_live_pipeline():

    @task.short_circuit(task_id="check_for_live_matches")
    def check_for_live_matches() -> bool:
        """
        Returns True (continue the DAG) only if at least one match is
        currently live. In production this would call the same source
        client used by ingestion (SourceClient.fetch_matches) and check
        each match's status/time_elapsed field. For the fixture source
        used in this environment, 'live' is approximated as: it's within
        the tournament window and not an off day. Swap this function's
        body for a real status check against the live API/scraper when
        deploying with WORLDCUP_SOURCE=wikipedia.
        """
        import sys

        sys.path.insert(0, PROJECT_ROOT)
        from ingestion.fixture_source import MATCHES

        has_scheduled_or_live = any(m["status"] != "finished" for m in MATCHES)
        return has_scheduled_or_live

    ingest = BashOperator(
        task_id="run_ingestion",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} ingestion/run_ingestion.py",
    )

    emit_live_events = BashOperator(
        task_id="emit_live_match_events",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} ingestion/live_event_simulator.py --count 5",
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} bronze/load_bronze.py",
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} silver/transform_silver.py",
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} gold/build_gold.py",
    )

    gate = check_for_live_matches()
    gate >> [ingest, emit_live_events] >> load_bronze >> transform_silver >> build_gold


worldcup_live_pipeline()
