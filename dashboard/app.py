"""
World Cup 2026 live analytics dashboard.

Reads exclusively from the gold layer (plus silver match_events for the
live feed) -- no business logic lives here. If a number on this dashboard
looks wrong, the bug is in silver/gold, not in this file.

Run with:
    streamlit run dashboard/app.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from dashboard.data_access import (
    load_goals_by_matchday,
    load_matches,
    load_recent_events,
    load_standings,
    load_team_form,
    load_top_scorers,
)

st.set_page_config(
    page_title="World Cup 2026 — Live Pipeline Dashboard",
    page_icon="\u26bd",
    layout="wide",
)

st_autorefresh(interval=10_000, key="wc_dashboard_autorefresh")

# ---------------------------------------------------------------------------
# Theme: broadcast control-room. Dark base, amber "on-air" accent for live
# state, teal for steady-state/data, monospace for all numeric readouts so
# stat columns line up like a scoreboard rather than reading like prose.
# ---------------------------------------------------------------------------
PALETTE = {
    "bg": "#0B0E11",
    "panel": "#12161B",
    "panel_border": "#222831",
    "text": "#E7E9EA",
    "text_dim": "#8A9099",
    "amber": "#F2A623",
    "teal": "#2BB3A3",
    "red": "#E2574C",
    "grid": "#1C2127",
}

CUSTOM_CSS = f"""
<style>
.stApp {{
    background-color: {PALETTE['bg']};
}}
[data-testid="stMetricValue"] {{
    font-family: "JetBrains Mono", "SF Mono", Consolas, monospace;
}}
.wc-panel {{
    background-color: {PALETTE['panel']};
    border: 1px solid {PALETTE['panel_border']};
    border-radius: 6px;
    padding: 1rem 1.25rem;
    margin-bottom: 0.75rem;
}}
.wc-live-dot {{
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background-color: {PALETTE['amber']};
    margin-right: 6px;
    animation: wc-pulse 1.6s ease-in-out infinite;
}}
@keyframes wc-pulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.35; }}
}}
.wc-eyebrow {{
    font-family: "JetBrains Mono", monospace;
    font-size: 0.72rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: {PALETTE['text_dim']};
}}
.wc-event-row {{
    font-family: "JetBrains Mono", monospace;
    font-size: 0.85rem;
    padding: 4px 0;
    border-bottom: 1px solid {PALETTE['grid']};
    color: {PALETTE['text']};
}}
.wc-event-minute {{
    color: {PALETTE['amber']};
    font-weight: 600;
    margin-right: 8px;
}}
.wc-pipeline-strip {{
    display: flex;
    gap: 4px;
    font-family: "JetBrains Mono", monospace;
    font-size: 0.72rem;
    color: {PALETTE['text_dim']};
}}
.wc-pipeline-stage {{
    flex: 1;
    text-align: center;
    padding: 6px 4px;
    border-radius: 4px;
    background-color: {PALETTE['panel']};
    border: 1px solid {PALETTE['teal']};
    color: {PALETTE['teal']};
}}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

EVENT_ICONS = {
    "goal": "\u26bd",
    "yellow_card": "\U0001f7e8",
    "red_card": "\U0001f7e5",
    "substitution": "\U0001f504",
}


def render_pipeline_status_strip():
    stages = ["ingest", "bronze", "silver", "gold", "dashboard"]
    cols = st.columns(len(stages))
    for col, stage in zip(cols, stages):
        with col:
            st.markdown(
                f'<div class="wc-pipeline-stage">{stage}</div>',
                unsafe_allow_html=True,
            )


def render_header():
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(
            '<span class="wc-eyebrow"><span class="wc-live-dot"></span>' "LIVE PIPELINE</span>",
            unsafe_allow_html=True,
        )
        st.title("World Cup 2026 \u2014 Live Analytics")
    with col2:
        st.markdown(
            f'<div style="text-align:right; padding-top:1.5rem;">'
            f'<span class="wc-eyebrow">last refresh</span><br>'
            f'<span style="font-family:monospace; color:{PALETTE["teal"]}">'
            f'{time.strftime("%H:%M:%S UTC", time.gmtime())}</span></div>',
            unsafe_allow_html=True,
        )


def render_standings(standings: pd.DataFrame):
    st.markdown('<p class="wc-eyebrow">group standings</p>', unsafe_allow_html=True)
    if standings.empty:
        st.info(
            "No standings yet \u2014 run the pipeline (ingestion \u2192 bronze \u2192 silver \u2192 gold) first."
        )
        return

    groups = sorted(standings["group"].dropna().unique())
    selected_group = st.selectbox("Group", groups, label_visibility="collapsed")

    group_df = (
        standings[standings["group"] == selected_group]
        .sort_values("group_rank")[
            [
                "group_rank",
                "team",
                "played",
                "won",
                "drawn",
                "lost",
                "goals_for",
                "goals_against",
                "goal_difference",
                "points",
            ]
        ]
        .rename(
            columns={
                "group_rank": "#",
                "team": "Team",
                "played": "P",
                "won": "W",
                "drawn": "D",
                "lost": "L",
                "goals_for": "GF",
                "goals_against": "GA",
                "goal_difference": "GD",
                "points": "Pts",
            }
        )
    )
    st.dataframe(group_df, hide_index=True, width="stretch")


def render_top_scorers(scorers: pd.DataFrame):
    st.markdown('<p class="wc-eyebrow">top scorers</p>', unsafe_allow_html=True)
    if scorers.empty:
        st.caption("No goal events yet.")
        return
    top = scorers.sort_values("goals", ascending=False).head(8)
    fig = go.Figure(
        go.Bar(
            x=top["goals"],
            y=[f"{row.player} ({row.team})" for row in top.itertuples()],
            orientation="h",
            marker_color=PALETTE["amber"],
        )
    )
    fig.update_layout(
        paper_bgcolor=PALETTE["panel"],
        plot_bgcolor=PALETTE["panel"],
        font_color=PALETTE["text"],
        margin=dict(l=10, r=10, t=10, b=10),
        height=280,
        yaxis=dict(autorange="reversed", gridcolor=PALETTE["grid"]),
        xaxis=dict(gridcolor=PALETTE["grid"], dtick=1),
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def render_goals_trend(trend: pd.DataFrame):
    st.markdown('<p class="wc-eyebrow">goals per matchday</p>', unsafe_allow_html=True)
    if trend.empty:
        st.caption("No finished matches yet.")
        return
    fig = go.Figure(
        go.Scatter(
            x=trend["matchday"],
            y=trend["avg_goals_per_match"],
            mode="lines+markers",
            line=dict(color=PALETTE["teal"], width=2),
            marker=dict(size=8, color=PALETTE["teal"]),
        )
    )
    fig.update_layout(
        paper_bgcolor=PALETTE["panel"],
        plot_bgcolor=PALETTE["panel"],
        font_color=PALETTE["text"],
        margin=dict(l=10, r=10, t=10, b=10),
        height=280,
        xaxis=dict(title="matchday", gridcolor=PALETTE["grid"], dtick=1),
        yaxis=dict(title="avg goals / match", gridcolor=PALETTE["grid"]),
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def render_live_feed(events: pd.DataFrame):
    st.markdown(
        '<p class="wc-eyebrow"><span class="wc-live-dot"></span>live match events</p>',
        unsafe_allow_html=True,
    )
    if events.empty:
        st.caption("No match events yet. Run ingestion/live_event_simulator.py to generate some.")
        return
    rows_html = ""
    for row in events.itertuples():
        icon = EVENT_ICONS.get(row.event_type, "\u2022")
        rows_html += (
            f'<div class="wc-event-row">'
            f'<span class="wc-event-minute">{row.minute}\'</span>'
            f"{icon} {row.player} ({row.team}) \u2014 {row.home_team} vs {row.away_team}"
            f"</div>"
        )
    st.markdown(f'<div class="wc-panel">{rows_html}</div>', unsafe_allow_html=True)


def render_team_form(form: pd.DataFrame):
    st.markdown('<p class="wc-eyebrow">recent form</p>', unsafe_allow_html=True)
    if form.empty:
        st.caption("No completed matches yet.")
        return
    display = form.copy()
    display["form"] = display["form"].apply(lambda s: " ".join(s) if isinstance(s, str) else s)
    st.dataframe(
        display.rename(columns={"team": "Team", "form": "Form (oldest \u2192 newest)"}),
        hide_index=True,
        width="stretch",
        height=240,
    )


def main():
    render_header()
    render_pipeline_status_strip()
    st.divider()

    standings = load_standings()
    scorers = load_top_scorers()
    trend = load_goals_by_matchday()
    form = load_team_form()
    events = load_recent_events(limit=12)
    matches = load_matches()

    if standings.empty:
        st.warning(
            "No gold data found. Run the pipeline first:\n\n"
            "```\npython ingestion/run_ingestion.py\n"
            "python bronze/load_bronze.py\n"
            "python silver/transform_silver.py\n"
            "python gold/build_gold.py\n```"
        )
        return

    finished = int((matches["status"] == "finished").sum()) if not matches.empty else 0
    total = len(matches) if not matches.empty else 0
    total_goals = int(trend["goals"].sum()) if not trend.empty else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Matches played", f"{finished} / {total}")
    m2.metric("Goals scored", total_goals)
    m3.metric("Teams tracked", standings["team"].nunique())
    m4.metric(
        "Avg goals / match",
        f"{trend['avg_goals_per_match'].mean():.2f}" if not trend.empty else "\u2014",
    )

    st.divider()

    left, right = st.columns([3, 2])
    with left:
        render_standings(standings)
        st.markdown("<br>", unsafe_allow_html=True)
        render_team_form(form)
    with right:
        render_live_feed(events)
        st.markdown("<br>", unsafe_allow_html=True)
        render_top_scorers(scorers)
        st.markdown("<br>", unsafe_allow_html=True)
        render_goals_trend(trend)

    st.divider()
    st.caption(
        "Data: bronze \u2192 silver \u2192 gold on Delta Lake (PySpark transforms, "
        "delta-rs I/O). Standings use SCD Type 2 in silver \u2014 every recompute "
        "preserves prior history rather than overwriting it."
    )


if __name__ == "__main__":
    main()
