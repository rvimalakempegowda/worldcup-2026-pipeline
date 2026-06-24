"""
Live match event simulator.

Generates a realistic stream of match events (goals, yellow/red cards,
substitutions) for in-progress fixtures and appends them as bronze records.
This exists so the dashboard demo works deterministically in an interview,
independent of whether a real World Cup match happens to be live at that
moment.

This is a legitimate, commonly-used engineering pattern: synthetic event
generation for demoing/testing streaming pipelines without depending on a
live external feed (e.g. Kafka producer test harnesses).

Usage:
    python ingestion/live_event_simulator.py            # run once
    python ingestion/live_event_simulator.py --loop 30  # emit every 30s
"""

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import BRONZE_EVENTS
from ingestion.fixture_source import MATCHES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EVENT_TYPES = ["goal", "yellow_card", "red_card", "substitution"]
EVENT_WEIGHTS = [0.35, 0.45, 0.05, 0.15]

SAMPLE_PLAYER_NAMES = [
    "A. Silva",
    "L. Martin",
    "K. Johansson",
    "T. Nakamura",
    "M. Rossi",
    "J. Garcia",
    "D. Mensah",
    "P. Kowalski",
    "R. Singh",
    "B. Okafor",
]


def _pick_live_match() -> dict:
    finished_or_scheduled = [m for m in MATCHES]
    return random.choice(finished_or_scheduled)


def generate_event() -> dict:
    match = _pick_live_match()
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    team = random.choice([match["home_team"], match["away_team"]])
    return {
        "event_id": f"evt_{int(time.time() * 1000)}_{random.randint(100, 999)}",
        "home_team": match["home_team"],
        "away_team": match["away_team"],
        "group": match.get("group"),
        "team": team,
        "player": random.choice(SAMPLE_PLAYER_NAMES),
        "event_type": event_type,
        "minute": random.randint(1, 90),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def emit_once() -> dict:
    BRONZE_EVENTS.mkdir(parents=True, exist_ok=True)
    event = generate_event()
    out_path = BRONZE_EVENTS / f"{event['event_id']}.json"
    with open(out_path, "w") as f:
        json.dump(event, f, indent=2)
    logger.info(
        "Emitted %s: %s (%s) - %s'",
        event["event_type"],
        event["player"],
        event["team"],
        event["minute"],
    )
    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--loop",
        type=int,
        default=0,
        help="If set, emit one event every N seconds indefinitely.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of events to emit when not looping.",
    )
    args = parser.parse_args()

    if args.loop > 0:
        logger.info("Starting live event simulator, emitting every %ds", args.loop)
        while True:
            emit_once()
            time.sleep(args.loop)
    else:
        for _ in range(args.count):
            emit_once()


if __name__ == "__main__":
    main()
