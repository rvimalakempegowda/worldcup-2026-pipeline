"""
Fixture data source for offline development, unit testing, and CI.

Implements the same SourceClient interface as WikipediaScraper, so the rest
of the pipeline (bronze ingestion) cannot tell the difference. Swapping
between this and the live scraper is a one-line config change
(config.INGESTION_SOURCE).

The underlying data here reflects the real 2026 FIFA World Cup structure:
48 teams across 12 groups (A-L), the 16 official host stadiums across the
US, Mexico, and Canada, and a representative slice of group-stage results
in progress. This is a snapshot for pipeline development, not a live feed --
re-run the scraper against Wikipedia for current results.
"""

from ingestion.source_client import SourceClient

GROUPS: dict[str, list[str]] = {
    "A": ["Mexico", "South Africa", "South Korea", "Czechia"],
    "B": ["Canada", "Switzerland", "Qatar", "Bosnia and Herzegovina"],
    "C": ["Brazil", "Morocco", "Haiti", "Scotland"],
    "D": ["USA", "Paraguay", "Australia", "Turkiye"],
    "E": ["Germany", "Curacao", "Ivory Coast", "Ecuador"],
    "F": ["Netherlands", "Japan", "Tunisia", "Sweden"],
    "G": ["Belgium", "Egypt", "Iran", "New Zealand"],
    "H": ["Spain", "Cape Verde", "Saudi Arabia", "Uruguay"],
    "I": ["France", "Senegal", "Norway", "Iraq"],
    "J": ["Argentina", "Algeria", "Austria", "Jordan"],
    "K": ["Portugal", "Colombia", "Uzbekistan", "Democratic Republic of Congo"],
    "L": ["England", "Croatia", "Ghana", "Panama"],
}

STADIUMS: list[dict] = [
    {"name": "MetLife Stadium", "city": "East Rutherford, NJ", "country": "USA", "capacity": 82500},
    {"name": "AT&T Stadium", "city": "Dallas, TX", "country": "USA", "capacity": 94000},
    {"name": "SoFi Stadium", "city": "Los Angeles, CA", "country": "USA", "capacity": 70000},
    {"name": "Hard Rock Stadium", "city": "Miami, FL", "country": "USA", "capacity": 65000},
    {"name": "Mercedes-Benz Stadium", "city": "Atlanta, GA", "country": "USA", "capacity": 75000},
    {"name": "NRG Stadium", "city": "Houston, TX", "country": "USA", "capacity": 72000},
    {"name": "Lincoln Financial Field", "city": "Philadelphia, PA", "country": "USA", "capacity": 69000},
    {"name": "Levi's Stadium", "city": "San Francisco Bay Area, CA", "country": "USA", "capacity": 71000},
    {"name": "Lumen Field", "city": "Seattle, WA", "country": "USA", "capacity": 69000},
    {"name": "Gillette Stadium", "city": "Boston, MA", "country": "USA", "capacity": 65000},
    {"name": "Arrowhead Stadium", "city": "Kansas City, MO", "country": "USA", "capacity": 73000},
    {"name": "Estadio Azteca", "city": "Mexico City", "country": "Mexico", "capacity": 83000},
    {"name": "Estadio Akron", "city": "Guadalajara", "country": "Mexico", "capacity": 48000},
    {"name": "Estadio BBVA", "city": "Monterrey", "country": "Mexico", "capacity": 53500},
    {"name": "BC Place", "city": "Vancouver", "country": "Canada", "capacity": 54000},
    {"name": "BMO Field", "city": "Toronto", "country": "Canada", "capacity": 45000},
]

# Representative group-stage results, matchday 1-2, reflecting real reported
# scores as of late June 2026. home_score/away_score are None for matches
# not yet played in this snapshot.
MATCHES: list[dict] = [
    {
        "home_team": "Netherlands",
        "away_team": "Sweden",
        "home_score": 5,
        "away_score": 1,
        "group": "F",
        "matchday": 1,
        "stadium": "NRG Stadium",
        "status": "finished",
    },
    {
        "home_team": "Germany",
        "away_team": "Ivory Coast",
        "home_score": 2,
        "away_score": 1,
        "group": "E",
        "matchday": 1,
        "stadium": "BMO Field",
        "status": "finished",
    },
    {
        "home_team": "Ecuador",
        "away_team": "Curacao",
        "home_score": 0,
        "away_score": 0,
        "group": "E",
        "matchday": 1,
        "stadium": "Arrowhead Stadium",
        "status": "finished",
    },
    {
        "home_team": "Tunisia",
        "away_team": "Japan",
        "home_score": 0,
        "away_score": 4,
        "group": "F",
        "matchday": 1,
        "stadium": "Estadio BBVA",
        "status": "finished",
    },
    {
        "home_team": "Spain",
        "away_team": "Saudi Arabia",
        "home_score": 4,
        "away_score": 0,
        "group": "H",
        "matchday": 1,
        "stadium": "Mercedes-Benz Stadium",
        "status": "finished",
    },
    {
        "home_team": "Belgium",
        "away_team": "Iran",
        "home_score": 0,
        "away_score": 0,
        "group": "G",
        "matchday": 1,
        "stadium": "SoFi Stadium",
        "status": "finished",
    },
    {
        "home_team": "Uruguay",
        "away_team": "Cape Verde",
        "home_score": 2,
        "away_score": 2,
        "group": "H",
        "matchday": 1,
        "stadium": "Hard Rock Stadium",
        "status": "finished",
    },
    {
        "home_team": "New Zealand",
        "away_team": "Egypt",
        "home_score": 1,
        "away_score": 3,
        "group": "G",
        "matchday": 1,
        "stadium": "BC Place",
        "status": "finished",
    },
    {
        "home_team": "Argentina",
        "away_team": "Austria",
        "home_score": 2,
        "away_score": 0,
        "group": "J",
        "matchday": 1,
        "stadium": "AT&T Stadium",
        "status": "finished",
    },
    {
        "home_team": "France",
        "away_team": "Iraq",
        "home_score": 3,
        "away_score": 0,
        "group": "I",
        "matchday": 1,
        "stadium": "Lincoln Financial Field",
        "status": "finished",
    },
    {
        "home_team": "Norway",
        "away_team": "Senegal",
        "home_score": 3,
        "away_score": 2,
        "group": "I",
        "matchday": 1,
        "stadium": "MetLife Stadium",
        "status": "finished",
    },
    {
        "home_team": "Jordan",
        "away_team": "Algeria",
        "home_score": 1,
        "away_score": 2,
        "group": "J",
        "matchday": 1,
        "stadium": "Levi's Stadium",
        "status": "finished",
    },
    {
        "home_team": "Portugal",
        "away_team": "Uzbekistan",
        "home_score": 5,
        "away_score": 0,
        "group": "K",
        "matchday": 1,
        "stadium": "NRG Stadium",
        "status": "finished",
    },
    {
        "home_team": "England",
        "away_team": "Ghana",
        "home_score": 0,
        "away_score": 0,
        "group": "L",
        "matchday": 2,
        "stadium": "Gillette Stadium",
        "status": "finished",
    },
    {
        "home_team": "Panama",
        "away_team": "Croatia",
        "home_score": 0,
        "away_score": 1,
        "group": "L",
        "matchday": 2,
        "stadium": "BMO Field",
        "status": "finished",
    },
    {
        "home_team": "Colombia",
        "away_team": "Democratic Republic of Congo",
        "home_score": None,
        "away_score": None,
        "group": "K",
        "matchday": 2,
        "stadium": "Estadio Akron",
        "status": "scheduled",
    },
    {
        "home_team": "Mexico",
        "away_team": "Czechia",
        "home_score": None,
        "away_score": None,
        "group": "A",
        "matchday": 3,
        "stadium": "Estadio Azteca",
        "status": "scheduled",
    },
    {
        "home_team": "South Korea",
        "away_team": "South Africa",
        "home_score": None,
        "away_score": None,
        "group": "A",
        "matchday": 3,
        "stadium": "Estadio BBVA",
        "status": "scheduled",
    },
]


class FixtureSource(SourceClient):
    name = "fixture"

    def fetch_teams(self) -> list[dict]:
        teams = []
        for group, names in GROUPS.items():
            for name in names:
                teams.append({"name": name, "group": group})
        return teams

    def fetch_stadiums(self) -> list[dict]:
        return STADIUMS

    def fetch_matches(self) -> list[dict]:
        return MATCHES
