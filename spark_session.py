"""
Shared Spark session builder.

On Databricks, the cluster already has an active SparkSession — calling
getOrCreate() returns it without reconfiguring. Local configs (shuffle
partitions, driver memory, UI) are skipped on the cluster to avoid
overriding tuning set by Databricks or the job definition.

Locally / CI, a minimal session is created with settings sized for a
single-machine run.
"""

import os

from pyspark.sql import SparkSession

from config import APP_NAME

_ON_DATABRICKS = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def get_spark() -> SparkSession:
    if _ON_DATABRICKS:
        return SparkSession.builder.appName(APP_NAME).getOrCreate()

    return (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
