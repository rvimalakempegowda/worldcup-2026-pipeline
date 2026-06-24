"""
Shared configuration for the World Cup 2026 pipeline.

In a Databricks deployment, LOCAL_DATA_ROOT becomes an ADLS Gen2 path, e.g.:
    "abfss://worldcup@<storage_account>.dfs.core.windows.net"
Everything else (table names, partitioning) stays the same.
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

LOCAL_DATA_ROOT = Path(os.environ.get("WORLDCUP_DATA_ROOT", PROJECT_ROOT / "data"))

RAW_DIR = LOCAL_DATA_ROOT / "raw"
BRONZE_DIR = LOCAL_DATA_ROOT / "bronze"
SILVER_DIR = LOCAL_DATA_ROOT / "silver"
GOLD_DIR = LOCAL_DATA_ROOT / "gold"

# Bronze tables
BRONZE_MATCHES = BRONZE_DIR / "matches"
BRONZE_TEAMS = BRONZE_DIR / "teams"
BRONZE_STADIUMS = BRONZE_DIR / "stadiums"
BRONZE_EVENTS = BRONZE_DIR / "match_events"

# Silver tables
SILVER_MATCHES = SILVER_DIR / "matches"
SILVER_TEAMS = SILVER_DIR / "teams"
SILVER_STANDINGS = SILVER_DIR / "group_standings"  # SCD2
SILVER_EVENTS = SILVER_DIR / "match_events"

# Gold tables
GOLD_STANDINGS_CURRENT = GOLD_DIR / "group_standings_current"
GOLD_TOP_SCORERS = GOLD_DIR / "top_scorers"
GOLD_TEAM_FORM = GOLD_DIR / "team_form"
GOLD_GOALS_BY_MATCHDAY = GOLD_DIR / "goals_by_matchday"

# Ingestion source: "fixture" (offline-safe, default) or "wikipedia" (live scrape)
INGESTION_SOURCE = os.environ.get("WORLDCUP_SOURCE", "fixture")

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/2026_FIFA_World_Cup"

APP_NAME = "worldcup2026-pipeline"
