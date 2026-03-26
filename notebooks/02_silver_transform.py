# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — NYC Taxi Cleaning & Transformation
# MAGIC Reads Bronze Delta table, applies:
# MAGIC - Type casting (string → timestamp, correct numerics)
# MAGIC - Null / invalid value filtering with audit logging
# MAGIC - Deduplication via window-function row_number
# MAGIC - Derived columns (trip_duration_minutes, speed_mph, fare_per_mile)
# MAGIC - MERGE INTO Silver for idempotent upserts

# COMMAND ----------

import logging
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("database", "nyc_taxi")
dbutils.widgets.text("bronze_table", "bronze_trips")
dbutils.widgets.text("silver_table", "silver_trips")
dbutils.widgets.text("batch_date", datetime.utcnow().strftime("%Y-%m-%d"))

CATALOG = dbutils.widgets.get("catalog")
DATABASE = dbutils.widgets.get("database")
BRONZE_TABLE = f"{CATALOG}.{DATABASE}.{dbutils.widgets.get('bronze_table')}"
SILVER_TABLE = f"{CATALOG}.{DATABASE}.{dbutils.widgets.get('silver_table')}"
BATCH_DATE = dbutils.widgets.get("batch_date")

logger.info(f"Bronze source : {BRONZE_TABLE}")
logger.info(f"Silver target : {SILVER_TABLE}")
logger.info(f"Batch date    : {BATCH_DATE}")

# COMMAND ----------

# MAGIC %md ## 1. Read Bronze

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
bronze_count = bronze_df.count()
logger.info(f"Bronze rows read: {bronze_count:,}")

# COMMAND ----------

# MAGIC %md ## 2. Cast & Rename Columns

# COMMAND ----------

TIMESTAMP_FMT = "yyyy-MM-dd HH:mm:ss"

typed_df = (
    bronze_df.withColumn(
        "pickup_datetime", F.to_timestamp(F.col("tpep_pickup_datetime"), TIMESTAMP_FMT)
    )
    .withColumn(
        "dropoff_datetime",
        F.to_timestamp(F.col("tpep_dropoff_datetime"), TIMESTAMP_FMT),
    )
    .withColumn("vendor_id", F.col("VendorID").cast(IntegerType()))
    .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
    .withColumn("ratecode_id", F.col("RatecodeID").cast(IntegerType()))
    .withColumn("pu_location_id", F.col("PULocationID").cast(IntegerType()))
    .withColumn("do_location_id", F.col("DOLocationID").cast(IntegerType()))
    .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
    .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
    .withColumn("extra", F.col("extra").cast(DoubleType()))
    .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))
    .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
    .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
    .withColumn(
        "improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType())
    )
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    .withColumn(
        "congestion_surcharge", F.col("congestion_surcharge").cast(DoubleType())
    )
    .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast(StringType()))
    # Retain audit cols from Bronze
    .withColumn("_bronze_ingestion_ts", F.col("_ingestion_timestamp"))
    .withColumn("_source_file", F.col("_source_file"))
)

# COMMAND ----------

# MAGIC %md ## 3. Filter Invalid Records

# COMMAND ----------

# Log counts before filtering
pre_filter_count = typed_df.count()

# Business rules from NYC TLC data dictionary
valid_df = (
    typed_df.filter(F.col("pickup_datetime").isNotNull())
    .filter(F.col("dropoff_datetime").isNotNull())
    .filter(F.col("pickup_datetime") < F.col("dropoff_datetime"))
    .filter(F.col("passenger_count").between(1, 9))
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("trip_distance") < 500)  # sanity cap
    .filter(F.col("fare_amount") >= 2.50)  # NYC minimum fare
    .filter(F.col("fare_amount") < 1000)  # sanity cap
    .filter(F.col("total_amount") > 0)
    .filter(F.col("tip_amount") >= 0)
    .filter(F.col("pu_location_id").isNotNull())
    .filter(F.col("do_location_id").isNotNull())
    .filter(F.col("payment_type").isin([1, 2, 3, 4, 5, 6]))  # TLC payment codes
)

post_filter_count = valid_df.count()
rejected_count = pre_filter_count - post_filter_count
rejection_rate = rejected_count / pre_filter_count * 100 if pre_filter_count > 0 else 0

logger.info(f"Pre-filter rows  : {pre_filter_count:,}")
logger.info(f"Post-filter rows : {post_filter_count:,}")
logger.info(f"Rejected rows    : {rejected_count:,}  ({rejection_rate:.2f}%)")

# Fail pipeline if rejection rate exceeds threshold
REJECTION_THRESHOLD_PCT = 20.0
assert rejection_rate < REJECTION_THRESHOLD_PCT, (
    f"Rejection rate {rejection_rate:.2f}% exceeds threshold {REJECTION_THRESHOLD_PCT}%. "
    "Investigate Bronze data quality before proceeding."
)

# COMMAND ----------

# MAGIC %md ## 4. Deduplication

# COMMAND ----------

# Deduplicate: keep the most-recently-ingested record per natural key
dedup_window = Window.partitionBy(
    "vendor_id", "pickup_datetime", "pu_location_id", "do_location_id"
).orderBy(F.col("_bronze_ingestion_ts").desc())

deduped_df = (
    valid_df.withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

dedup_count = deduped_df.count()
logger.info(
    f"After dedup: {dedup_count:,} rows (removed {post_filter_count - dedup_count:,} duplicates)"
)

# COMMAND ----------

# MAGIC %md ## 5. Derived / Enrichment Columns

# COMMAND ----------

enriched_df = (
    deduped_df.withColumn(
        "trip_duration_minutes",
        F.round(
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
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
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))  # 1=Sun … 7=Sat
    .withColumn("pickup_month", F.month("pickup_datetime"))
    .withColumn("pickup_year", F.year("pickup_datetime"))
    # Payment type description
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
    # Silver audit columns
    .withColumn("_silver_processed_ts", F.current_timestamp())
    .withColumn("_batch_date", F.lit(BATCH_DATE))
)

# COMMAND ----------

# MAGIC %md ## 6. Select Final Silver Schema

# COMMAND ----------

SILVER_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_date",
    "pickup_hour",
    "pickup_dow",
    "pickup_month",
    "pickup_year",
    "passenger_count",
    "trip_distance",
    "trip_duration_minutes",
    "speed_mph",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "payment_type_desc",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tip_percentage",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "fare_per_mile",
    "_bronze_ingestion_ts",
    "_source_file",
    "_silver_processed_ts",
    "_batch_date",
]

silver_df = enriched_df.select(SILVER_COLUMNS)

# COMMAND ----------

# MAGIC %md ## 7. Create Silver Table (if not exists) & MERGE

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")

# Create table with partitioning and Delta properties
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        vendor_id               INT,
        pickup_datetime         TIMESTAMP,
        dropoff_datetime        TIMESTAMP,
        pickup_date             DATE,
        pickup_hour             INT,
        pickup_dow              INT,
        pickup_month            INT,
        pickup_year             INT,
        passenger_count         INT,
        trip_distance           DOUBLE,
        trip_duration_minutes   DOUBLE,
        speed_mph               DOUBLE,
        ratecode_id             INT,
        store_and_fwd_flag      STRING,
        pu_location_id          INT,
        do_location_id          INT,
        payment_type            INT,
        payment_type_desc       STRING,
        fare_amount             DOUBLE,
        extra                   DOUBLE,
        mta_tax                 DOUBLE,
        tip_amount              DOUBLE,
        tip_percentage          DOUBLE,
        tolls_amount            DOUBLE,
        improvement_surcharge   DOUBLE,
        total_amount            DOUBLE,
        congestion_surcharge    DOUBLE,
        fare_per_mile           DOUBLE,
        _bronze_ingestion_ts    TIMESTAMP,
        _source_file            STRING,
        _silver_processed_ts    TIMESTAMP,
        _batch_date             STRING
    )
    USING DELTA
    PARTITIONED BY (pickup_year, pickup_month)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.enableChangeDataFeed'       = 'true',
        'delta.minReaderVersion'           = '1',
        'delta.minWriterVersion'           = '4'
    )
    """
)

# MERGE — upsert on natural key
silver_delta = DeltaTable.forName(spark, SILVER_TABLE)

(
    silver_delta.alias("target")
    .merge(
        silver_df.alias("source"),
        """
        target.vendor_id       = source.vendor_id       AND
        target.pickup_datetime = source.pickup_datetime  AND
        target.pu_location_id  = source.pu_location_id  AND
        target.do_location_id  = source.do_location_id
        """,
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

silver_count = spark.table(SILVER_TABLE).count()
logger.info(f"Silver table '{SILVER_TABLE}' now contains {silver_count:,} rows.")

# COMMAND ----------

# MAGIC %md ## 8. Optimize & Vacuum

# COMMAND ----------

spark.sql(
    f"OPTIMIZE {SILVER_TABLE} ZORDER BY (pickup_datetime, pu_location_id, do_location_id)"
)
spark.sql(f"VACUUM {SILVER_TABLE} RETAIN 168 HOURS")  # 7 days
spark.sql(f"ANALYZE TABLE {SILVER_TABLE} COMPUTE STATISTICS FOR ALL COLUMNS")
logger.info("Optimize / Vacuum / Analyze complete.")

# COMMAND ----------

dbutils.notebook.exit(
    str(
        {
            "silver_table": SILVER_TABLE,
            "silver_row_count": silver_count,
            "rejected_rows": rejected_count,
            "rejection_rate_pct": round(rejection_rate, 2),
            "batch_date": BATCH_DATE,
            "completed_at": datetime.utcnow().isoformat(),
        }
    )
)
