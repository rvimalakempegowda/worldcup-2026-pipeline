"""
Data access layer for the dashboard.

Deliberately does not start a SparkSession -- by the time data reaches the
gold layer it's small, pre-aggregated, and tabular, so reading it through
delta-rs straight into pandas is faster and lighter than spinning up a JVM
just to display a dashboard. This mirrors how a real BI tool (Power BI,
Tableau, Databricks SQL) would connect to gold tables directly rather than
going through Spark.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from config import (
    GOLD_GOALS_BY_MATCHDAY,
    GOLD_STANDINGS_CURRENT,
    GOLD_TEAM_FORM,
    GOLD_TOP_SCORERS,
    SILVER_EVENTS,
    SILVER_MATCHES,
)


def _read_delta(path: Path) -> pd.DataFrame:
    if not path.exists() or not (path / "_delta_log").exists():
        return pd.DataFrame()
    return DeltaTable(str(path)).to_pandas()


@st.cache_data(ttl=15, show_spinner=False)
def load_standings() -> pd.DataFrame:
    return _read_delta(GOLD_STANDINGS_CURRENT)


@st.cache_data(ttl=15, show_spinner=False)
def load_top_scorers() -> pd.DataFrame:
    return _read_delta(GOLD_TOP_SCORERS)


@st.cache_data(ttl=15, show_spinner=False)
def load_team_form() -> pd.DataFrame:
    return _read_delta(GOLD_TEAM_FORM)


@st.cache_data(ttl=15, show_spinner=False)
def load_goals_by_matchday() -> pd.DataFrame:
    return _read_delta(GOLD_GOALS_BY_MATCHDAY)


@st.cache_data(ttl=10, show_spinner=False)
def load_recent_events(limit: int = 15) -> pd.DataFrame:
    df = _read_delta(SILVER_EVENTS)
    if df.empty:
        return df
    return df.sort_values("ingested_at", ascending=False).head(limit)


@st.cache_data(ttl=15, show_spinner=False)
def load_matches() -> pd.DataFrame:
    return _read_delta(SILVER_MATCHES)


def data_freshness() -> dict:
    """Returns the most recent ingested_at timestamp found across gold
    inputs, used to show a 'last updated' indicator in the UI."""
    standings = load_standings()
    if standings.empty:
        return {"available": False}
    return {"available": True}
