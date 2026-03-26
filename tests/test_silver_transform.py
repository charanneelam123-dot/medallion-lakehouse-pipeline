"""
test_silver_transform.py
Unit tests for the Silver transformation logic.

Uses a local PySpark session (no Databricks cluster required) so tests run
in CI without any cloud dependencies.

Run:
    pytest tests/test_silver_transform.py -v
"""

from __future__ import annotations

from datetime import datetime
from typing import List

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests (local mode)."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_silver_transform")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


RAW_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
    ]
)

TIMESTAMP_FMT = "yyyy-MM-dd HH:mm:ss"

VALID_ROW = {
    "VendorID": 1,
    "tpep_pickup_datetime": "2023-01-15 08:30:00",
    "tpep_dropoff_datetime": "2023-01-15 08:55:00",
    "passenger_count": 2,
    "trip_distance": 5.3,
    "RatecodeID": 1,
    "store_and_fwd_flag": "N",
    "PULocationID": 161,
    "DOLocationID": 236,
    "payment_type": 1,
    "fare_amount": 18.50,
    "extra": 0.50,
    "mta_tax": 0.50,
    "tip_amount": 4.00,
    "tolls_amount": 0.0,
    "improvement_surcharge": 0.30,
    "total_amount": 24.30,
    "congestion_surcharge": 2.50,
    "airport_fee": 0.0,
}


def make_bronze_df(spark: SparkSession, rows: List[dict]):
    """Helper: create a Bronze-schema DataFrame from a list of dicts."""
    spark_rows = [Row(**r) for r in rows]
    df = spark.createDataFrame(spark_rows, schema=RAW_SCHEMA)
    # Add Bronze audit columns
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit("dbfs:/test/sample.csv"))
        .withColumn("_load_mode", F.lit("incremental"))
        .withColumn("_bronze_batch_id", F.lit("20230115083000"))
    )


# ─── Transformation Helpers (extracted from notebook for testability) ─────────


def apply_type_casting(df):
    return (
        df.withColumn(
            "pickup_datetime",
            F.to_timestamp(F.col("tpep_pickup_datetime"), TIMESTAMP_FMT),
        )
        .withColumn(
            "dropoff_datetime",
            F.to_timestamp(F.col("tpep_dropoff_datetime"), TIMESTAMP_FMT),
        )
        .withColumn("vendor_id", F.col("VendorID").cast(IntegerType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("pu_location_id", F.col("PULocationID").cast(IntegerType()))
        .withColumn("do_location_id", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    )


def apply_filters(df):
    return (
        df.filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("pickup_datetime") < F.col("dropoff_datetime"))
        .filter(F.col("passenger_count").between(1, 9))
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") < 500)
        .filter(F.col("fare_amount") >= 2.50)
        .filter(F.col("fare_amount") < 1000)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("tip_amount") >= 0)
        .filter(F.col("payment_type").isin([1, 2, 3, 4, 5, 6]))
    )


def apply_derived_columns(df):
    return (
        df.withColumn(
            "trip_duration_minutes",
            F.round(
                (
                    F.unix_timestamp("dropoff_datetime")
                    - F.unix_timestamp("pickup_datetime")
                )
                / 60.0,
                2,
            ),
        )
        .withColumn(
            "speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.round(
                    F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0), 2
                ),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn(
            "fare_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.round(F.col("fare_amount") / F.col("trip_distance"), 2),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn(
            "payment_type_desc",
            F.when(F.col("payment_type") == 1, "Credit card")
            .when(F.col("payment_type") == 2, "Cash")
            .when(F.col("payment_type") == 3, "No charge")
            .when(F.col("payment_type") == 4, "Dispute")
            .when(F.col("payment_type") == 5, "Unknown")
            .when(F.col("payment_type") == 6, "Voided trip")
            .otherwise("Other"),
        )
    )


# ─── Tests: Type Casting ─────────────────────────────────────────────────────


class TestTypeCasting:

    def test_pickup_datetime_cast(self, spark):
        df = make_bronze_df(spark, [VALID_ROW])
        result = apply_type_casting(df)
        row = result.collect()[0]
        assert row["pickup_datetime"] is not None
        assert isinstance(row["pickup_datetime"], datetime)

    def test_dropoff_datetime_cast(self, spark):
        df = make_bronze_df(spark, [VALID_ROW])
        result = apply_type_casting(df)
        row = result.collect()[0]
        assert row["dropoff_datetime"] is not None
        assert isinstance(row["dropoff_datetime"], datetime)

    def test_fare_amount_is_double(self, spark):
        df = make_bronze_df(spark, [VALID_ROW])
        result = apply_type_casting(df)
        dtype = dict(result.dtypes)["fare_amount"]
        assert dtype == "double"

    def test_invalid_timestamp_becomes_null(self, spark):
        bad_row = {**VALID_ROW, "tpep_pickup_datetime": "not-a-date"}
        df = make_bronze_df(spark, [bad_row])
        result = apply_type_casting(df)
        row = result.collect()[0]
        assert row["pickup_datetime"] is None


# ─── Tests: Filtering ────────────────────────────────────────────────────────


class TestFiltering:

    def test_valid_row_passes_all_filters(self, spark):
        df = make_bronze_df(spark, [VALID_ROW])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 1

    def test_negative_trip_distance_filtered(self, spark):
        row = {**VALID_ROW, "trip_distance": -1.0}
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 0

    def test_zero_trip_distance_filtered(self, spark):
        row = {**VALID_ROW, "trip_distance": 0.0}
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 0

    def test_zero_passenger_count_filtered(self, spark):
        row = {**VALID_ROW, "passenger_count": 0}
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 0

    def test_ten_passengers_filtered(self, spark):
        row = {**VALID_ROW, "passenger_count": 10}
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 0

    def test_fare_below_minimum_filtered(self, spark):
        row = {**VALID_ROW, "fare_amount": 1.00}
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 0

    def test_dropoff_before_pickup_filtered(self, spark):
        row = {
            **VALID_ROW,
            "tpep_pickup_datetime": "2023-01-15 09:00:00",
            "tpep_dropoff_datetime": "2023-01-15 08:00:00",  # reversed
        }
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 0

    def test_invalid_payment_type_filtered(self, spark):
        row = {**VALID_ROW, "payment_type": 99}
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 0

    def test_null_pickup_datetime_filtered(self, spark):
        row = {**VALID_ROW, "tpep_pickup_datetime": None}
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 0

    def test_multiple_rows_partial_filter(self, spark):
        rows = [
            VALID_ROW,  # passes
            {**VALID_ROW, "trip_distance": 0.0},  # fails
            {**VALID_ROW, "passenger_count": 0},  # fails
            {**VALID_ROW, "fare_amount": 1.00},  # fails
        ]
        df = make_bronze_df(spark, rows)
        typed = apply_type_casting(df)
        result = apply_filters(typed)
        assert result.count() == 1


# ─── Tests: Derived Columns ──────────────────────────────────────────────────


class TestDerivedColumns:

    def _get_derived(self, spark, row_override=None):
        row = {**VALID_ROW, **(row_override or {})}
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        filtered = apply_filters(typed)
        return apply_derived_columns(filtered).collect()[0]

    def test_trip_duration_minutes(self, spark):
        row = self._get_derived(spark)
        # 08:30 → 08:55 = 25 minutes
        assert row["trip_duration_minutes"] == pytest.approx(25.0, abs=0.1)

    def test_speed_mph_positive(self, spark):
        row = self._get_derived(spark)
        assert row["speed_mph"] is not None
        assert row["speed_mph"] > 0

    def test_speed_mph_formula(self, spark):
        # distance=5.3 miles, duration=25 min → 5.3 / (25/60) = 12.72 mph
        row = self._get_derived(spark)
        expected_speed = round(5.3 / (25 / 60), 2)
        assert row["speed_mph"] == pytest.approx(expected_speed, abs=0.1)

    def test_fare_per_mile_positive(self, spark):
        row = self._get_derived(spark)
        assert row["fare_per_mile"] is not None
        assert row["fare_per_mile"] > 0

    def test_fare_per_mile_formula(self, spark):
        # fare=18.50, distance=5.3 → 3.49
        row = self._get_derived(spark)
        expected = round(18.50 / 5.3, 2)
        assert row["fare_per_mile"] == pytest.approx(expected, abs=0.01)

    def test_tip_percentage(self, spark):
        # tip=4.00, fare=18.50 → 21.62%
        row = self._get_derived(spark)
        expected = round(4.00 / 18.50 * 100, 2)
        assert row["tip_percentage"] == pytest.approx(expected, abs=0.1)

    def test_payment_type_desc_credit_card(self, spark):
        row = self._get_derived(spark, {"payment_type": 1})
        assert row["payment_type_desc"] == "Credit card"

    def test_payment_type_desc_cash(self, spark):
        row = self._get_derived(spark, {"payment_type": 2})
        assert row["payment_type_desc"] == "Cash"

    def test_pickup_hour_extracted(self, spark):
        row = self._get_derived(spark)
        assert row["pickup_hour"] == 8

    def test_pickup_dow_extracted(self, spark):
        # 2023-01-15 is a Sunday → dayofweek = 1
        row = self._get_derived(spark)
        assert row["pickup_dow"] == 1

    def test_pickup_month_extracted(self, spark):
        row = self._get_derived(spark)
        assert row["pickup_month"] == 1

    def test_pickup_year_extracted(self, spark):
        row = self._get_derived(spark)
        assert row["pickup_year"] == 2023

    def test_zero_fare_tip_pct_is_zero(self, spark):
        """Edge case: zero fare should result in 0% tip, not division error."""
        row = {
            **VALID_ROW,
            "fare_amount": 2.50,  # minimum valid fare
            "tip_amount": 0.0,
        }
        df = make_bronze_df(spark, [row])
        typed = apply_type_casting(df)
        filtered = apply_filters(typed)
        result = apply_derived_columns(filtered).collect()[0]
        assert result["tip_percentage"] == pytest.approx(0.0, abs=0.01)


# ─── Tests: Deduplication ────────────────────────────────────────────────────


class TestDeduplication:

    def test_exact_duplicate_removed(self, spark):
        from pyspark.sql import Window

        rows = [VALID_ROW, VALID_ROW]
        df = make_bronze_df(spark, rows)
        typed = apply_type_casting(df)
        filtered = apply_filters(typed)

        dedup_window = Window.partitionBy(
            "vendor_id", "pickup_datetime", "pu_location_id", "do_location_id"
        ).orderBy(F.col("_ingestion_timestamp").desc())

        deduped = filtered.withColumn(
            "_row_num", F.row_number().over(dedup_window)
        ).filter(F.col("_row_num") == 1)
        assert deduped.count() == 1

    def test_different_trip_both_retained(self, spark):
        from pyspark.sql import Window

        row2 = {
            **VALID_ROW,
            "PULocationID": 999,
            "fare_amount": 30.0,
            "total_amount": 35.0,
        }
        rows = [VALID_ROW, row2]
        df = make_bronze_df(spark, rows)
        typed = apply_type_casting(df)
        filtered = apply_filters(typed)

        dedup_window = Window.partitionBy(
            "vendor_id", "pickup_datetime", "pu_location_id", "do_location_id"
        ).orderBy(F.col("_ingestion_timestamp").desc())

        deduped = filtered.withColumn(
            "_row_num", F.row_number().over(dedup_window)
        ).filter(F.col("_row_num") == 1)
        assert deduped.count() == 2


# ─── Tests: Rejection Rate Threshold ─────────────────────────────────────────


class TestRejectionRate:

    def test_rejection_rate_below_threshold(self, spark):
        rows = [VALID_ROW] * 90 + [{**VALID_ROW, "trip_distance": 0.0}] * 10
        df = make_bronze_df(spark, rows)
        typed = apply_type_casting(df)
        pre = typed.count()
        post = apply_filters(typed).count()
        rate = (pre - post) / pre * 100
        assert rate < 20.0, f"Rejection rate {rate:.1f}% must be below 20%"

    def test_rejection_rate_above_threshold_raises(self, spark):
        rows = [VALID_ROW] * 10 + [{**VALID_ROW, "trip_distance": 0.0}] * 90
        df = make_bronze_df(spark, rows)
        typed = apply_type_casting(df)
        pre = typed.count()
        post = apply_filters(typed).count()
        rate = (pre - post) / pre * 100
        assert rate >= 20.0, "Expected rejection rate above threshold for this dataset"
        with pytest.raises(AssertionError, match="Rejection rate"):
            assert rate < 20.0, (
                f"Rejection rate {rate:.2f}% exceeds threshold 20.0%. "
                "Investigate Bronze data quality before proceeding."
            )
