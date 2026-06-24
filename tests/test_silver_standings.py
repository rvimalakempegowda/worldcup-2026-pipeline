"""
Unit tests for silver standings computation.

These exercise `_compute_standings` directly against small, hand-built
DataFrames rather than the full bronze->silver pipeline, so failures point
precisely at the aggregation/ranking logic rather than at ingestion or I/O.
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from silver.transform_silver import _compute_standings

MATCH_SCHEMA = StructType(
    [
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("home_score", IntegerType(), True),
        StructField("away_score", IntegerType(), True),
        StructField("group", StringType(), True),
        StructField("matchday", IntegerType(), True),
        StructField("stadium", StringType(), True),
        StructField("status", StringType(), True),
        StructField("result", StringType(), True),
    ]
)


def _make_matches(spark, rows):
    return spark.createDataFrame(rows, schema=MATCH_SCHEMA)


def test_standings_points_for_win_draw_loss(spark):
    rows = [
        ("A", "B", 2, 0, "X", 1, "Stadium 1", "finished", "home_win"),
        ("C", "D", 1, 1, "X", 1, "Stadium 2", "finished", "draw"),
    ]
    matches = _make_matches(spark, rows)
    standings = _compute_standings(matches).orderBy("team").collect()

    by_team = {r["team"]: r for r in standings}

    assert by_team["A"]["points"] == 3
    assert by_team["A"]["won"] == 1
    assert by_team["B"]["points"] == 0
    assert by_team["B"]["lost"] == 1
    assert by_team["C"]["points"] == 1
    assert by_team["C"]["drawn"] == 1
    assert by_team["D"]["points"] == 1
    assert by_team["D"]["drawn"] == 1


def test_standings_goal_difference_and_ranking(spark):
    # Same group, same points (3 each), but A has a better goal difference
    # than C, so A should rank above C despite both having 1 win.
    rows = [
        ("A", "B", 4, 0, "X", 1, "Stadium 1", "finished", "home_win"),
        ("C", "D", 1, 0, "X", 1, "Stadium 2", "finished", "home_win"),
    ]
    matches = _make_matches(spark, rows)
    standings = _compute_standings(matches).orderBy("group_rank").collect()
    by_team = {r["team"]: r for r in standings}

    assert by_team["A"]["goal_difference"] == 4
    assert by_team["C"]["goal_difference"] == 1
    assert by_team["A"]["group_rank"] < by_team["C"]["group_rank"]


def test_standings_excludes_unfinished_matches(spark):
    rows = [
        ("A", "B", None, None, "X", 1, "Stadium 1", "scheduled", None),
    ]
    matches = _make_matches(spark, rows)
    standings = _compute_standings(matches).collect()
    assert len(standings) == 0


def test_standings_played_count_across_multiple_matchdays(spark):
    rows = [
        ("A", "B", 1, 0, "X", 1, "Stadium 1", "finished", "home_win"),
        ("C", "A", 2, 2, "X", 2, "Stadium 2", "finished", "draw"),
    ]
    matches = _make_matches(spark, rows)
    standings = _compute_standings(matches).collect()
    by_team = {r["team"]: r for r in standings}

    assert by_team["A"]["played"] == 2
    assert by_team["A"]["points"] == 4  # 3 for the win + 1 for the draw
    assert by_team["A"]["goals_for"] == 3
    assert by_team["A"]["goals_against"] == 2
