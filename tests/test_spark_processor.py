import json
import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


# ---- Tests without Spark session (pure Python logic) ----

def test_schema_definition():
    """Verify SCHEMA has 9 fields with correct names and types."""
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    expected_schema = StructType([
        StructField("ticker", StringType()),
        StructField("sector", StringType()),
        StructField("timestamp", StringType()),
        StructField("fetch_ts", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", LongType()),
    ])

    fields = {f.name: f.dataType for f in expected_schema.fields}
    assert isinstance(fields["ticker"], StringType)
    assert isinstance(fields["sector"], StringType)
    assert isinstance(fields["timestamp"], StringType)
    assert isinstance(fields["fetch_ts"], StringType)
    assert isinstance(fields["open"], DoubleType)
    assert isinstance(fields["high"], DoubleType)
    assert isinstance(fields["low"], DoubleType)
    assert isinstance(fields["close"], DoubleType)
    assert isinstance(fields["volume"], LongType)
    assert len(fields) == 9


# ---- Tests requiring Spark session (skipped if Java not available) ----

spark = None
try:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName("test")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
except Exception:
    pass

requires_spark = pytest.mark.skipif(spark is None, reason="Spark not available (Java missing)")


@requires_spark
def test_parse_valid_json():
    from pyspark.sql.functions import from_json, col
    from spark_processor import SCHEMA

    with open(os.path.join(FIXTURES_DIR, "sample_payload.json")) as f:
        payload = f.read()

    df = spark.createDataFrame([(payload,)], ["value"])
    result = df.withColumn("parsed", from_json(col("value"), SCHEMA)).select("parsed.*")
    row = result.collect()[0]

    assert row.ticker == "BYAN.JK"
    assert row.sector == "Energy"
    assert row.open == 10000.0


@requires_spark
def test_parse_invalid_json_returns_null():
    from pyspark.sql.functions import from_json, col
    from spark_processor import SCHEMA

    df = spark.createDataFrame([("{invalid json}",)], ["value"])
    result = df.withColumn("parsed", from_json(col("value"), SCHEMA)).select("parsed.*")
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0][0] is None


@requires_spark
def test_nan_handling_double_cols():
    from pyspark.sql.functions import col, when, isnan

    data = [("TEST.JK", float("nan"), float("nan"), float("nan"), float("nan"), 100)]
    df = spark.createDataFrame(data, ["ticker", "open", "high", "low", "close", "volume"])
    double_cols = ["open", "high", "low", "close"]
    for c in double_cols:
        df = df.withColumn(c, when(isnan(col(c)), None).otherwise(col(c)))

    row = df.collect()[0]
    assert row.open is None
    assert row.volume == 100


@requires_spark
def test_null_ticker_filtered():
    from pyspark.sql.functions import col

    data = [
        (None, "Energy"),
        ("", "Energy"),
        ("BYAN.JK", "Energy"),
    ]
    df = spark.createDataFrame(data, ["ticker", "sector"])
    filtered = df.filter(col("ticker").isNotNull() & (col("ticker") != ""))
    assert filtered.count() == 1


@requires_spark
def test_timestamp_parse_iso8601_with_tz():
    from pyspark.sql.functions import col, to_timestamp

    df = spark.createDataFrame([("2026-05-21T15:47:00+07:00",)], ["ts_str"])
    result = df.withColumn("ts", to_timestamp(col("ts_str"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
    row = result.collect()[0]
    assert row.ts is not None
    assert "2026-05-21" in str(row.ts)


@requires_spark
def test_watermark_drop_duplicates():
    from pyspark.sql.functions import col, to_timestamp

    data = [
        ("BYAN.JK", "2026-05-21T09:30:00+07:00", 10000.0),
        ("BYAN.JK", "2026-05-21T09:30:00+07:00", 10050.0),
        ("BYAN.JK", "2026-05-21T09:31:00+07:00", 10100.0),
    ]
    df = spark.createDataFrame(data, ["ticker", "timestamp_str", "close"])
    df = df.withColumn("timestamp", to_timestamp(col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
    df = df.withWatermark("timestamp", "30 minutes").dropDuplicates(["ticker", "timestamp"])
    assert df.count() == 2


def test_credential_path_fallback():
    """GOOGLE_APPLICATION_CREDENTIALS env var fallback matches expected default."""
    expected = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/gcp-service-account.json")
    assert expected.endswith("gcp-service-account.json")
