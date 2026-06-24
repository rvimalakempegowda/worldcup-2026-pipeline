"""
Unit tests for the ingestion source layer.

These check the contract that matters most for this project's design:
any SourceClient implementation (fixture, scraper, future paid API) must
return data shaped the same way, so swapping sources never requires
touching bronze/silver/gold.
"""

from ingestion.fixture_source import GROUPS, STADIUMS, FixtureSource
from ingestion.source_client import IngestionResult, SourceClient


def test_fixture_source_implements_source_client_interface():
    assert isinstance(FixtureSource(), SourceClient)


def test_fixture_source_returns_48_teams_across_12_groups():
    teams = FixtureSource().fetch_teams()
    assert len(teams) == 48
    groups_seen = {t["group"] for t in teams}
    assert groups_seen == set(GROUPS.keys())


def test_fixture_source_returns_16_stadiums():
    stadiums = FixtureSource().fetch_stadiums()
    assert len(stadiums) == len(STADIUMS) == 16
    for stadium in stadiums:
        assert "name" in stadium
        assert "city" in stadium


def test_fixture_source_matches_have_required_keys():
    matches = FixtureSource().fetch_matches()
    assert len(matches) > 0
    required_keys = {"home_team", "away_team", "group", "matchday", "status"}
    for match in matches:
        assert required_keys.issubset(match.keys())


def test_fetch_all_returns_ingestion_result_with_source_name():
    result = FixtureSource().fetch_all()
    assert isinstance(result, IngestionResult)
    assert result.source_name == "fixture"
    assert len(result.teams) == 48
    assert len(result.stadiums) == 16
    assert len(result.matches) > 0
    assert result.fetched_at  # non-empty timestamp string


def test_no_duplicate_team_names_across_groups():
    teams = FixtureSource().fetch_teams()
    names = [t["name"] for t in teams]
    assert len(names) == len(set(names)), "Duplicate team name found across groups"
