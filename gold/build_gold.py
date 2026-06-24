"""
Gold layer: aggregated, BI-ready Delta tables built from silver.

These are the tables the dashboard (and, on Databricks, Databricks SQL /
Power BI / Tableau) reads directly. No further joins or business logic
should be needed downstream of gold -- that work happens here.

Usage:
    python gold/build_gold.py
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from config import (
    GOLD_GOALS_BY_MATCHDAY,
    GOLD_STANDINGS_CURRENT,
    GOLD_TEAM_FORM,
    GOLD_TOP_SCORERS,
    SILVER_EVENTS,
    SILVER_MATCHES,
    SILVER_STANDINGS,
)
from delta_io import delta_table_exists, read_delta_table, write_delta_table
from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_gold_standings_current(spark):
    silver = read_delta_table(spark, SILVER_STANDINGS)
    current = (
        silver.filter(F.col("is_current"))
        .select(
            "group",
            "team",
            "played",
            "won",
            "drawn",
            "lost",
            "goals_for",
            "goals_against",
            "goal_difference",
            "points",
            "group_rank",
        )
        .orderBy("group", "group_rank")
    )
    write_delta_table(current, GOLD_STANDINGS_CURRENT, mode="overwrite")
    logger.info("Gold group_standings_current: %d rows", current.count())
    return current


def build_gold_top_scorers(spark):
    if not delta_table_exists(SILVER_EVENTS):
        logger.info("No silver events table yet, skipping top scorers")
        return None
    events = read_delta_table(spark, SILVER_EVENTS)
    goals = events.filter(F.col("event_type") == "goal")
    top_scorers = (
        goals.groupBy("player", "team").agg(F.count("*").alias("goals")).orderBy(F.col("goals").desc())
    )
    write_delta_table(top_scorers, GOLD_TOP_SCORERS, mode="overwrite")
    logger.info("Gold top_scorers: %d rows", top_scorers.count())
    return top_scorers


def build_gold_team_form(spark):
    """Last result per team, in match order, as a compact form string e.g. 'WDL'."""
    silver = read_delta_table(spark, SILVER_MATCHES)
    finished = silver.filter(F.col("status") == "finished")

    home = finished.select(
        F.col("home_team").alias("team"),
        F.col("matchday"),
        F.when(F.col("result") == "home_win", "W")
        .when(F.col("result") == "away_win", "L")
        .otherwise("D")
        .alias("outcome"),
    )
    away = finished.select(
        F.col("away_team").alias("team"),
        F.col("matchday"),
        F.when(F.col("result") == "away_win", "W")
        .when(F.col("result") == "home_win", "L")
        .otherwise("D")
        .alias("outcome"),
    )
    combined = home.unionByName(away)

    form = (
        combined.groupBy("team")
        .agg(F.sort_array(F.collect_list(F.struct("matchday", "outcome"))).alias("ordered"))
        .withColumn("form", F.concat_ws("", F.col("ordered.outcome")))
        .select("team", "form")
    )
    write_delta_table(form, GOLD_TEAM_FORM, mode="overwrite")
    logger.info("Gold team_form: %d rows", form.count())
    return form


def build_gold_goals_by_matchday(spark):
    silver = read_delta_table(spark, SILVER_MATCHES)
    finished = silver.filter(F.col("status") == "finished")
    trend = (
        finished.withColumn("total_goals", F.col("home_score") + F.col("away_score"))
        .groupBy("matchday")
        .agg(
            F.sum("total_goals").alias("goals"),
            F.count("*").alias("matches_played"),
        )
        .withColumn("avg_goals_per_match", F.round(F.col("goals") / F.col("matches_played"), 2))
        .orderBy("matchday")
    )
    write_delta_table(trend, GOLD_GOALS_BY_MATCHDAY, mode="overwrite")
    logger.info("Gold goals_by_matchday: %d rows", trend.count())
    return trend


def run():
    spark = get_spark()
    build_gold_standings_current(spark)
    build_gold_top_scorers(spark)
    build_gold_team_form(spark)
    build_gold_goals_by_matchday(spark)
    spark.stop()
    logger.info("Gold build complete.")


if __name__ == "__main__":
    run()
