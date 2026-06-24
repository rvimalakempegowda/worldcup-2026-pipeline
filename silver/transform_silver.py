"""
Silver layer: clean, conform, and dedupe bronze data into typed Delta tables.

Group standings (`group_standings`) use SCD Type 2: every time standings are
recomputed, the previous "current" row for a team is closed out
(`is_current = False`, `effective_to = now`) and a new row is inserted
(`is_current = True`, `effective_to = NULL`). This preserves full history --
you can query what the table looked like after any matchday, the same
pattern used for zone-lookup history in the taxi streaming project.

Usage:
    python silver/transform_silver.py
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import Window
from pyspark.sql import functions as F

from config import (
    BRONZE_EVENTS,
    BRONZE_MATCHES,
    BRONZE_TEAMS,
    SILVER_EVENTS,
    SILVER_MATCHES,
    SILVER_STANDINGS,
    SILVER_TEAMS,
)
from delta_io import delta_table_exists, read_delta_table, write_delta_table
from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_silver_teams(spark):
    bronze = read_delta_table(spark, BRONZE_TEAMS)
    silver = (
        bronze.dropDuplicates(["name"])
        .withColumn("name", F.trim(F.col("name")))
        .filter(F.col("name").isNotNull() & (F.col("name") != ""))
        .select("name", "group", "ingested_at", "source_name")
    )
    write_delta_table(silver, SILVER_TEAMS, mode="overwrite")
    logger.info("Silver teams: %d rows", silver.count())
    return silver


def build_silver_matches(spark):
    bronze = read_delta_table(spark, BRONZE_MATCHES)
    silver = (
        bronze.dropDuplicates(["home_team", "away_team", "matchday"])
        .withColumn("home_score", F.col("home_score").cast("int"))
        .withColumn("away_score", F.col("away_score").cast("int"))
        .withColumn("matchday", F.col("matchday").cast("int"))
        .withColumn(
            "result",
            F.when(F.col("status") != "finished", F.lit(None))
            .when(F.col("home_score") > F.col("away_score"), F.lit("home_win"))
            .when(F.col("home_score") < F.col("away_score"), F.lit("away_win"))
            .otherwise(F.lit("draw")),
        )
        .select(
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "group",
            "matchday",
            "stadium",
            "status",
            "result",
            "ingested_at",
            "source_name",
        )
    )
    write_delta_table(silver, SILVER_MATCHES, mode="overwrite")
    logger.info("Silver matches: %d rows", silver.count())
    return silver


def build_silver_events(spark):
    events_path = BRONZE_EVENTS.parent / "match_events_delta"
    if not delta_table_exists(events_path):
        logger.info("No bronze match_events table yet, skipping silver events")
        return None
    bronze = read_delta_table(spark, events_path)
    silver = bronze.dropDuplicates(["event_id"]).withColumn("minute", F.col("minute").cast("int"))
    write_delta_table(silver, SILVER_EVENTS, mode="overwrite")
    logger.info("Silver match_events: %d rows", silver.count())
    return silver


def _compute_standings(matches_df):
    """Compute group standings (W/D/L, GF, GA, GD, Pts) from finished matches."""
    finished = matches_df.filter(F.col("status") == "finished")

    home = finished.select(
        F.col("home_team").alias("team"),
        F.col("group"),
        F.col("home_score").alias("gf"),
        F.col("away_score").alias("ga"),
        F.col("result"),
        F.lit("home").alias("venue"),
    )
    away = finished.select(
        F.col("away_team").alias("team"),
        F.col("group"),
        F.col("away_score").alias("gf"),
        F.col("home_score").alias("ga"),
        F.col("result"),
        F.lit("away").alias("venue"),
    )
    combined = home.unionByName(away)

    combined = combined.withColumn(
        "points",
        F.when((F.col("venue") == "home") & (F.col("result") == "home_win"), 3)
        .when((F.col("venue") == "away") & (F.col("result") == "away_win"), 3)
        .when(F.col("result") == "draw", 1)
        .otherwise(0),
    )
    combined = combined.withColumn(
        "is_win",
        F.when(
            ((F.col("venue") == "home") & (F.col("result") == "home_win"))
            | ((F.col("venue") == "away") & (F.col("result") == "away_win")),
            1,
        ).otherwise(0),
    )
    combined = combined.withColumn("is_draw", F.when(F.col("result") == "draw", 1).otherwise(0))
    combined = combined.withColumn(
        "is_loss",
        F.when(
            ((F.col("venue") == "home") & (F.col("result") == "away_win"))
            | ((F.col("venue") == "away") & (F.col("result") == "home_win")),
            1,
        ).otherwise(0),
    )

    standings = combined.groupBy("group", "team").agg(
        F.count("*").alias("played"),
        F.sum("is_win").alias("won"),
        F.sum("is_draw").alias("drawn"),
        F.sum("is_loss").alias("lost"),
        F.sum("gf").alias("goals_for"),
        F.sum("ga").alias("goals_against"),
        F.sum("points").alias("points"),
    )
    standings = standings.withColumn("goal_difference", F.col("goals_for") - F.col("goals_against"))

    rank_window = Window.partitionBy("group").orderBy(
        F.col("points").desc(), F.col("goal_difference").desc(), F.col("goals_for").desc()
    )
    standings = standings.withColumn("group_rank", F.rank().over(rank_window))
    return standings


def build_silver_standings_scd2(spark, matches_df):
    """SCD Type 2 merge: close out changed/removed rows, insert new current rows."""
    new_standings = _compute_standings(matches_df)
    now = datetime.now(timezone.utc).isoformat()

    new_standings = (
        new_standings.withColumn("effective_from", F.lit(now))
        .withColumn("effective_to", F.lit(None).cast("string"))
        .withColumn("is_current", F.lit(True))
    )

    if not delta_table_exists(SILVER_STANDINGS):
        write_delta_table(new_standings, SILVER_STANDINGS, mode="overwrite")
        logger.info("Silver group_standings (initial load): %d rows", new_standings.count())
        return new_standings

    existing = read_delta_table(spark, SILVER_STANDINGS)
    existing_current = existing.filter(F.col("is_current"))
    existing_history = existing.filter(~F.col("is_current"))

    compare_cols = ["played", "won", "drawn", "lost", "goals_for", "goals_against", "points", "group_rank"]

    joined = existing_current.alias("old").join(
        new_standings.alias("new"), on=["group", "team"], how="full_outer"
    )

    changed_keys = joined.filter(
        F.col("old.team").isNull()
        | F.col("new.team").isNull()
        | F.expr(" OR ".join(f"old.{c} IS DISTINCT FROM new.{c}" for c in compare_cols))
    ).select(
        F.coalesce(F.col("old.group"), F.col("new.group")).alias("group"),
        F.coalesce(F.col("old.team"), F.col("new.team")).alias("team"),
    )

    rows_to_close = existing_current.join(changed_keys, on=["group", "team"], how="inner")
    rows_to_close = rows_to_close.withColumn("is_current", F.lit(False)).withColumn(
        "effective_to", F.lit(now)
    )

    rows_unchanged = existing_current.join(changed_keys, on=["group", "team"], how="left_anti")

    rows_to_insert = new_standings.join(
        rows_unchanged.select("group", "team"), on=["group", "team"], how="left_anti"
    )

    final = (
        existing_history.unionByName(rows_to_close).unionByName(rows_unchanged).unionByName(rows_to_insert)
    )

    write_delta_table(final, SILVER_STANDINGS, mode="overwrite")
    logger.info(
        "Silver group_standings SCD2: %d unchanged, %d closed, %d new -> %d total rows",
        rows_unchanged.count(),
        rows_to_close.count(),
        rows_to_insert.count(),
        final.count(),
    )
    return final


def run():
    spark = get_spark()
    build_silver_teams(spark)
    matches_df = build_silver_matches(spark)
    build_silver_events(spark)
    build_silver_standings_scd2(spark, matches_df)
    spark.stop()
    logger.info("Silver transform complete.")


if __name__ == "__main__":
    run()
