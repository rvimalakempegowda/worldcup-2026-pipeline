"""
Delta Lake I/O for Spark DataFrames.

Two paths:
- On Databricks (DATABRICKS_RUNTIME_VERSION set): native Spark Delta
  (`df.write.format("delta")`). The runtime ships with the Delta JARs and
  Unity Catalog integration pre-wired — no extra dependencies needed.
- Locally / CI: delta-rs (`deltalake` PyPI package). JVM-free, reads the
  same on-disk format, avoids Maven Central network requirements.

All transformation logic above this module is unchanged between environments.
"""

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

_ON_DATABRICKS = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


# ── Databricks-native path ────────────────────────────────────────────────────

def _write_native(df: DataFrame, path: Path, mode: str) -> int:
    count = df.count()
    df.write.format("delta").mode(mode).save(str(path))
    return count


def _read_native(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(str(path))


def _exists_native(path: Path) -> bool:
    # On Databricks, UC volume paths are FUSE-mounted POSIX paths.
    return path.exists() and (path / "_delta_log").exists()


# ── Local / CI path (delta-rs) ────────────────────────────────────────────────

def _write_deltalake(df: DataFrame, path: Path, mode: str) -> int:
    from deltalake import write_deltalake
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    write_deltalake(str(path), pdf, mode=mode)
    return len(pdf)


def _arrow_type_to_spark(arrow_type):
    import pyarrow as pa
    from pyspark.sql import types as T

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type) or pa.types.is_null(arrow_type):
        return T.StringType()
    if pa.types.is_boolean(arrow_type):
        return T.BooleanType()
    if pa.types.is_integer(arrow_type):
        return T.LongType()
    if pa.types.is_floating(arrow_type):
        return T.DoubleType()
    if pa.types.is_timestamp(arrow_type):
        return T.TimestampType()
    return T.StringType()


def _read_deltalake(spark: SparkSession, path: Path) -> DataFrame:
    from deltalake import DeltaTable
    from pyspark.sql import types as T

    dt = DeltaTable(str(path))
    arrow_table = dt.to_pyarrow_table()
    spark_schema = T.StructType(
        [T.StructField(f.name, _arrow_type_to_spark(f.type), True) for f in arrow_table.schema]
    )
    pdf = arrow_table.to_pandas()
    pdf = pdf.astype(object).where(pdf.notna(), None)
    return spark.createDataFrame(pdf.to_dict("records"), schema=spark_schema)


def _exists_deltalake(path: Path) -> bool:
    return path.exists() and (path / "_delta_log").exists()


# ── Public API ────────────────────────────────────────────────────────────────

def write_delta_table(df: DataFrame, path: Path, mode: str = "append") -> int:
    if _ON_DATABRICKS:
        return _write_native(df, path, mode)
    return _write_deltalake(df, path, mode)


def read_delta_table(spark: SparkSession, path: Path) -> DataFrame:
    if not delta_table_exists(path):
        raise FileNotFoundError(f"No Delta table found at {path}")
    if _ON_DATABRICKS:
        return _read_native(spark, path)
    return _read_deltalake(spark, path)


def delta_table_exists(path: Path) -> bool:
    if _ON_DATABRICKS:
        return _exists_native(path)
    return _exists_deltalake(path)
