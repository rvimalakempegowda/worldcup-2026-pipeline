"""
Daily maintenance DAG.

Handles work that should happen once a day regardless of whether a match
is currently live: refreshing reference data (teams, stadiums, groups --
data that almost never changes mid-tournament) and running data quality
checks against the gold layer so issues surface on a schedule rather than
only when someone happens to look at the dashboard.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

PROJECT_ROOT = "/home/claude/worldcup-pipeline"
PYTHON_BIN = "python3"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="worldcup_daily_maintenance",
    description="Daily reference data refresh and data quality checks",
    schedule="0 6 * * *",  # once a day at 06:00
    start_date=datetime(2026, 6, 11),
    end_date=datetime(2026, 7, 20),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["worldcup", "maintenance", "data-quality"],
)
def worldcup_daily_maintenance():

    refresh_reference_data = BashOperator(
        task_id="refresh_reference_data",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} ingestion/run_ingestion.py",
    )

    rebuild_gold = BashOperator(
        task_id="rebuild_gold",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} gold/build_gold.py",
    )

    @task
    def run_data_quality_checks() -> dict:
        """
        Lightweight DQ checks against gold tables: row-count sanity bounds,
        null-key checks, and referential consistency (every team in
        standings should exist in the teams table). Raises if a check
        fails, which fails the task and (per default_args) triggers a
        retry/alert -- this is the same role Great Expectations or dbt
        tests would play in a heavier setup, kept dependency-light here.
        """
        import sys

        sys.path.insert(0, PROJECT_ROOT)
        from deltalake import DeltaTable

        from config import GOLD_STANDINGS_CURRENT, SILVER_TEAMS

        standings = DeltaTable(str(GOLD_STANDINGS_CURRENT)).to_pandas()
        teams = DeltaTable(str(SILVER_TEAMS)).to_pandas()

        results = {}

        results["standings_not_empty"] = len(standings) > 0
        results["standings_no_null_team"] = standings["team"].notna().all()
        results["standings_points_non_negative"] = (standings["points"] >= 0).all()

        unknown_teams = set(standings["team"]) - set(teams["name"])
        results["all_standings_teams_known"] = len(unknown_teams) == 0
        if unknown_teams:
            results["unknown_teams"] = sorted(unknown_teams)

        failed = [k for k, v in results.items() if k != "unknown_teams" and v is False]
        if failed:
            raise ValueError(f"Data quality checks failed: {failed}. Details: {results}")

        return results

    refresh_reference_data >> rebuild_gold >> run_data_quality_checks()


worldcup_daily_maintenance()
