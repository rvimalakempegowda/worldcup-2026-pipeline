"""
Ingestion entry point.

Selects a SourceClient implementation based on config.INGESTION_SOURCE and
writes the raw fetched payload to the landing zone (data/raw) as JSON,
stamped with an ingestion timestamp. This raw file is what bronze/load_bronze.py
reads.

Usage:
    python ingestion/run_ingestion.py
    WORLDCUP_SOURCE=wikipedia python ingestion/run_ingestion.py
"""

import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import INGESTION_SOURCE, RAW_DIR, WIKIPEDIA_URL
from ingestion.fixture_source import FixtureSource
from ingestion.source_client import SourceClient
from ingestion.wikipedia_scraper import WikipediaScraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_source_client() -> SourceClient:
    if INGESTION_SOURCE == "wikipedia":
        logger.info("Using live Wikipedia scraper")
        return WikipediaScraper(WIKIPEDIA_URL)
    logger.info("Using offline fixture source (set WORLDCUP_SOURCE=wikipedia for live scrape)")
    return FixtureSource()


def run() -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    client = get_source_client()
    result = client.fetch_all()

    out_path = RAW_DIR / f"ingestion_{result.fetched_at.replace(':', '-')}.json"
    with open(out_path, "w") as f:
        json.dump(asdict(result), f, indent=2)

    logger.info(
        "Ingested %d teams, %d stadiums, %d matches from source=%s -> %s",
        len(result.teams),
        len(result.stadiums),
        len(result.matches),
        result.source_name,
        out_path,
    )

    # Also write/update a "latest" pointer for downstream stages.
    latest_path = RAW_DIR / "latest.json"
    with open(latest_path, "w") as f:
        json.dump(asdict(result), f, indent=2)

    return out_path


if __name__ == "__main__":
    run()
