"""
Bronze layer: land raw ingested data into Delta tables, as-is.

Bronze is append-only and schema-on-read in spirit -- we apply a light
explicit schema only so Spark doesn't have to infer types from JSON on
every read, but we do not clean, dedupe, or reshape anything here. That
happens in silver/transform_silver.py.

Usage:
    python bronze/load_bronze.py
"""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from config import (
    BRONZE_EVENTS,
    BRONZE_MATCHES,
    BRONZE_STADIUMS,
    BRONZE_TEAMS,
    RAW_DIR,
)
from delta_io import write_delta_table
from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _load_latest_raw() -> dict:
    latest_path = RAW_DIR / "latest.json"
    if not latest_path.exists():
        raise FileNotFoundError(
            f"No raw ingestion found at {latest_path}. " "Run `python ingestion/run_ingestion.py` first."
        )
    with open(latest_path) as f:
        return json.load(f)


def load_bronze_teams(spark, payload: dict):
    teams = payload["teams"]
    if not teams:
        logger.warning("No teams in raw payload, skipping bronze teams write")
        return
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("group", StringType(), True),
            StructField("source_table", StringType(), True),
        ]
    )
    # Normalize records to the declared schema's field set so mixed-source
    # payloads (fixture has "group", scraper has "source_table") don't blow up.
    normalized = [{field.name: rec.get(field.name) for field in schema.fields} for rec in teams]
    df = spark.createDataFrame(normalized, schema=schema)
    df = df.withColumn("ingested_at", F.lit(payload["fetched_at"]))
    df = df.withColumn("source_name", F.lit(payload["source_name"]))
    n = write_delta_table(df, BRONZE_TEAMS, mode="append")
    logger.info("Wrote %d rows to bronze teams", n)


def load_bronze_stadiums(spark, payload: dict):
    stadiums = payload["stadiums"]
    if not stadiums:
        logger.warning("No stadiums in raw payload, skipping bronze stadiums write")
        return
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("capacity", StringType(), True),
        ]
    )
    normalized = [
        {
            field.name: str(rec.get(field.name)) if rec.get(field.name) is not None else None
            for field in schema.fields
        }
        for rec in stadiums
    ]
    df = spark.createDataFrame(normalized, schema=schema)
    df = df.withColumn("capacity", F.col("capacity").cast("int"))
    df = df.withColumn("ingested_at", F.lit(payload["fetched_at"]))
    df = df.withColumn("source_name", F.lit(payload["source_name"]))
    n = write_delta_table(df, BRONZE_STADIUMS, mode="append")
    logger.info("Wrote %d rows to bronze stadiums", n)


def load_bronze_matches(spark, payload: dict):
    matches = payload["matches"]
    if not matches:
        logger.warning("No matches in raw payload, skipping bronze matches write")
        return
    schema = StructType(
        [
            StructField("home_team", StringType(), True),
            StructField("away_team", StringType(), True),
            StructField("home_score", StringType(), True),
            StructField("away_score", StringType(), True),
            StructField("group", StringType(), True),
            StructField("matchday", StringType(), True),
            StructField("stadium", StringType(), True),
            StructField("status", StringType(), True),
            StructField("score", StringType(), True),
        ]
    )
    normalized = [
        {
            field.name: (str(rec.get(field.name)) if rec.get(field.name) is not None else None)
            for field in schema.fields
        }
        for rec in matches
    ]
    df = spark.createDataFrame(normalized, schema=schema)
    df = df.withColumn("ingested_at", F.lit(payload["fetched_at"]))
    df = df.withColumn("source_name", F.lit(payload["source_name"]))
    n = write_delta_table(df, BRONZE_MATCHES, mode="append")
    logger.info("Wrote %d rows to bronze matches", n)


def load_bronze_events(spark):
    """Match events are written one-file-per-event by the live simulator
    (see ingestion/live_event_simulator.py). We sweep that directory here
    rather than receiving them inline in the ingestion payload, mirroring
    how a real streaming source (Kafka topic) would land into bronze
    separately from the batch scrape."""
    event_files = list(BRONZE_EVENTS.glob("*.json")) if BRONZE_EVENTS.exists() else []
    if not event_files:
        logger.info("No match events found yet, skipping bronze events write")
        return

    records = []
    for f in event_files:
        with open(f) as fh:
            records.append(json.load(fh))

    df = spark.createDataFrame(records)
    delta_table_path = BRONZE_EVENTS.parent / "match_events_delta"
    n = write_delta_table(df, delta_table_path, mode="append")
    logger.info("Wrote %d rows to bronze match_events", n)


def run():
    spark = get_spark()
    payload = _load_latest_raw()
    load_bronze_teams(spark, payload)
    load_bronze_stadiums(spark, payload)
    load_bronze_matches(spark, payload)
    load_bronze_events(spark)
    spark.stop()
    logger.info("Bronze load complete.")


if __name__ == "__main__":
    run()
