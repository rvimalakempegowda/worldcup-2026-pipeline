"""
Shared Spark session builder.

All transformation logic in this project (bronze/silver/gold) runs on
PySpark DataFrames -- the exact same DataFrame API and transformation code
that would run on a Databricks cluster. The only thing that differs between
this local setup and Databricks is *how Delta tables get written/read*:

- On Databricks: native `df.write.format("delta")`, because the runtime
  ships with the Delta JARs preinstalled and Unity Catalog configured.
- Here: native Spark's Delta JARs would normally be fetched from Maven
  Central at session startup (`configure_spark_with_delta_pip`), which
  requires general internet egress. In network-restricted environments
  (this sandbox, some corporate CI runners) that download is blocked, so
  this project uses `deltalake` (the Rust-native delta-rs engine, installed
  from PyPI, no JVM/Maven dependency) as the Delta read/write layer --
  see `delta_io.py`. Spark still performs all transformations; only table
  I/O is routed through delta-rs via a Spark DataFrame <-> Arrow bridge.

This is a real, supported pattern (delta-rs is the same engine used by
Polars' and DuckDB's Delta integrations) and is documented in the README
as a deliberate adaptation to this environment's network policy.
"""

from pyspark.sql import SparkSession

from config import APP_NAME


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return builder.getOrCreate()
