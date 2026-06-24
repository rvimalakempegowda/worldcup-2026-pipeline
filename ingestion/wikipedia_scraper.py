"""
Live scraper for 2026 FIFA World Cup data from Wikipedia.

This is the production data source. It parses the public results tables on
the tournament's Wikipedia page using BeautifulSoup. Wikipedia's tables are
well-structured and stable across edits (consistent column headers), which
makes it a reasonable scrape target compared to a JS-rendered live-score
site -- no headless browser needed, and the page is updated quickly as
results come in.

Note: this module makes real outbound HTTP requests and will not run in
network-restricted environments (e.g. CI runners or sandboxes without
general internet egress). Use FixtureSource (fixture_source.py) in those
environments -- see config.INGESTION_SOURCE.
"""

import logging
import re
import time

import requests
from bs4 import BeautifulSoup

from ingestion.source_client import SourceClient

logger = logging.getLogger(__name__)

USER_AGENT = (
    "worldcup-pipeline-bot/1.0 (educational data engineering project; "
    "respects robots.txt; contact via GitHub issues)"
)

REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
BACKOFF_SECONDS = 2


class WikipediaScraper(SourceClient):
    name = "wikipedia"

    def __init__(self, url: str):
        self.url = url
        self._soup: BeautifulSoup | None = None

    def _get_soup(self) -> BeautifulSoup:
        if self._soup is not None:
            return self._soup

        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    self.url,
                    headers={"User-Agent": USER_AGENT},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                resp.raise_for_status()
                self._soup = BeautifulSoup(resp.text, "html.parser")
                return self._soup
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning("Fetch attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
                time.sleep(BACKOFF_SECONDS * attempt)

        raise RuntimeError(f"Failed to fetch {self.url} after {MAX_RETRIES} attempts") from last_exc

    def fetch_teams(self) -> list[dict]:
        soup = self._get_soup()
        teams = []
        # Wikipedia group tables are wikitables with a "Qualified teams" or
        # group-letter caption. We look for tables whose header row contains
        # "Team" and "Pts" - the standard FIFA group table shape.
        for table in soup.find_all("table", class_="wikitable"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if not any("Pts" in h or "Team" in h for h in headers):
                continue
            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                team_cell = cells[1] if len(cells) > 1 else cells[0]
                team_name = team_cell.get_text(strip=True)
                team_name = re.sub(r"\s*\([A-Za-z]+\)\s*$", "", team_name)
                if team_name and team_name not in [t["name"] for t in teams]:
                    teams.append({"name": team_name, "source_table": "group_stage"})
        return teams

    def fetch_stadiums(self) -> list[dict]:
        soup = self._get_soup()
        stadiums = []
        for table in soup.find_all("table", class_="wikitable"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if not any("Stadium" in h or "Venue" in h for h in headers):
                continue
            for row in table.find_all("tr")[1:]:
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= 2:
                    stadiums.append({"name": cells[0], "city": cells[1]})
        return stadiums

    def fetch_matches(self) -> list[dict]:
        soup = self._get_soup()
        matches = []
        for table in soup.find_all("table", class_="footballbox"):
            try:
                home = table.find("th", class_="fhome").get_text(strip=True)
                away = table.find("th", class_="faway").get_text(strip=True)
                score = table.find("th", class_="fscore").get_text(strip=True)
                matches.append({"home_team": home, "away_team": away, "score": score})
            except AttributeError:
                continue
        return matches
