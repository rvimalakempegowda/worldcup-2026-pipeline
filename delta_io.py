"""
Delta Lake I/O for Spark DataFrames, backed by delta-rs (the `deltalake`
PyPI package) instead of Spark's native JVM Delta connector.

Why: Spark's native Delta support (`df.write.format("delta")`) requires the
io.delta:delta-spark JAR, which `delta-spark`'s Python package fetches from
Maven Central at session startup. That works fine on Databricks (JARs are
preinstalled) and on any machine with normal internet access, but fails in
network-restricted environments where only an explicit domain allowlist
(PyPI, npm, GitHub, ...) is reachable and Maven Central is not.

`deltalake` (delta-rs) is a separate, JVM-free implementation of the Delta
Lake protocol, installable from PyPI, used as the Delta engine inside
Polars and DuckDB. It speaks the same table format on disk -- a Delta table
written by delta-rs is fully readable by Spark/Databricks and vice versa.

This module converts Spark DataFrames to/from Arrow (via pandas) at the
read/write boundary so all transformation logic upstream stays in PySpark.

On Databricks, replace calls to write_delta_table/read_delta_table with
native `df.write.format("delta").save(path)` / `spark.read.format("delta").load(path)`
-- the transformation code that calls these functions does not change.
"""

from pathlib import Path

from deltalake import DeltaTable, write_deltalake
from pyspark.sql import DataFrame, SparkSession


def write_delta_table(df: DataFrame, path: Path, mode: str = "append") -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    write_deltalake(str(path), pdf, mode=mode)
    return len(pdf)


def _arrow_type_to_spark(arrow_type) -> object:
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


def read_delta_table(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No Delta table found at {path}")
    dt = DeltaTable(str(path))
    arrow_table = dt.to_pyarrow_table()

    from pyspark.sql import types as T

    spark_schema = T.StructType(
        [T.StructField(field.name, _arrow_type_to_spark(field.type), True) for field in arrow_table.schema]
    )

    pdf = arrow_table.to_pandas()
    pdf = pdf.astype(object).where(pdf.notna(), None)
    records = pdf.to_dict("records")
    return spark.createDataFrame(records, schema=spark_schema)


def delta_table_exists(path: Path) -> bool:
    return path.exists() and (path / "_delta_log").exists()
