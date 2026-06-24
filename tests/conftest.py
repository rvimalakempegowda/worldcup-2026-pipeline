"""
Shared pytest fixtures.

A single SparkSession is created once per test session (not per test) --
creating a new JVM-backed SparkSession per test is slow and is the most
common reason PySpark test suites time out in CI. All tests that need Spark
take the `spark` fixture as an argument.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("worldcup-pipeline-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
