# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — NYC Taxi Raw Ingestion
# MAGIC Ingests NYC TLC Trip Record CSV data into a Delta Lake Bronze table.
# MAGIC - No transformations applied; raw data is preserved as-is.
# MAGIC - Audit columns (ingestion timestamp, source file) are appended.
# MAGIC - Idempotent via COPY INTO (auto-loader compatible fallback included).

# COMMAND ----------

import logging
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# Widget parameters — override at runtime via Databricks Jobs or notebook params
dbutils.widgets.text("catalog", "main", "Unity Catalog name")
dbutils.widgets.text("database", "nyc_taxi", "Database / schema name")
dbutils.widgets.text("bronze_table", "bronze_trips", "Bronze Delta table name")
dbutils.widgets.text(
    "source_path",
    "dbfs:/FileStore/nyc_taxi/raw/",
    "Source CSV root path (DBFS or ADLS)",
)
dbutils.widgets.text("load_mode", "incremental", "Load mode: full | incremental")

CATALOG = dbutils.widgets.get("catalog")
DATABASE = dbutils.widgets.get("database")
BRONZE_TABLE = dbutils.widgets.get("bronze_table")
SOURCE_PATH = dbutils.widgets.get("source_path")
LOAD_MODE = dbutils.widgets.get("load_mode")

FULLY_QUALIFIED_TABLE = f"{CATALOG}.{DATABASE}.{BRONZE_TABLE}"
CHECKPOINT_PATH = f"dbfs:/checkpoints/bronze/{BRONZE_TABLE}/_checkpoint"

logger.info(f"Target table : {FULLY_QUALIFIED_TABLE}")
logger.info(f"Source path  : {SOURCE_PATH}")
logger.info(f"Load mode    : {LOAD_MODE}")

# COMMAND ----------

# MAGIC %md ## 2. Raw Schema Definition
# MAGIC Mirrors the NYC TLC Yellow Taxi CSV layout (2019-present).

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md ## 3. Ensure Catalog / Database Exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")
logger.info(f"Catalog '{CATALOG}' and database '{DATABASE}' verified.")

# COMMAND ----------

# MAGIC %md ## 4. Ingestion — Auto Loader (Structured Streaming, trigger once)

# COMMAND ----------


def ingest_with_autoloader(
    source_path: str,
    target_table: str,
    schema: StructType,
    checkpoint_path: str,
    load_mode: str,
) -> int:
    """
    Use Databricks Auto Loader (cloudFiles) to ingest CSVs into the Bronze Delta table.
    Runs as a one-shot trigger so it can be orchestrated by Airflow without a persistent stream.
    Returns the number of new records written.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(schema)
        .load(source_path)
    )

    enriched = (
        reader.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_load_mode", F.lit(load_mode))
        .withColumn(
            "_bronze_batch_id", F.lit(datetime.utcnow().strftime("%Y%m%d%H%M%S"))
        )
    )

    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()
    logger.info("Auto Loader stream completed.")

    count = spark.table(target_table).count()
    return count


# COMMAND ----------

# MAGIC %md ## 5. Full Reload Fallback (non-streaming)

# COMMAND ----------


def ingest_full_reload(
    source_path: str,
    target_table: str,
    schema: StructType,
) -> int:
    """
    Truncate-and-reload path for full refresh runs.
    Uses COPY INTO for idempotent bulk loads.
    """
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table}
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'delta.enableChangeDataFeed'       = 'true'
        )
        """
    )

    spark.sql(
        f"""
        COPY INTO {target_table}
        FROM (
            SELECT
                *,
                current_timestamp()      AS _ingestion_timestamp,
                _metadata.file_path      AS _source_file,
                'full'                   AS _load_mode,
                '{datetime.utcnow().strftime("%Y%m%d%H%M%S")}' AS _bronze_batch_id
            FROM '{source_path}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
        COPY_OPTIONS ('mergeSchema' = 'true')
        """
    )

    count = spark.table(target_table).count()
    return count


# COMMAND ----------

# MAGIC %md ## 6. Run Ingestion

# COMMAND ----------

if LOAD_MODE == "full":
    logger.info("Running full reload via COPY INTO …")
    records_written = ingest_full_reload(SOURCE_PATH, FULLY_QUALIFIED_TABLE, RAW_SCHEMA)
else:
    logger.info("Running incremental Auto Loader ingestion …")
    records_written = ingest_with_autoloader(
        SOURCE_PATH,
        FULLY_QUALIFIED_TABLE,
        RAW_SCHEMA,
        CHECKPOINT_PATH,
        LOAD_MODE,
    )

logger.info(
    f"Bronze table '{FULLY_QUALIFIED_TABLE}' now contains {records_written:,} records."
)

# COMMAND ----------

# MAGIC %md ## 7. Post-Ingestion Optimization

# COMMAND ----------

spark.sql(f"OPTIMIZE {FULLY_QUALIFIED_TABLE} ZORDER BY (tpep_pickup_datetime)")
spark.sql(f"ANALYZE TABLE {FULLY_QUALIFIED_TABLE} COMPUTE STATISTICS FOR ALL COLUMNS")
logger.info("OPTIMIZE + ANALYZE complete.")

# COMMAND ----------

# MAGIC %md ## 8. Data Quality Gate — Row Count Assert

# COMMAND ----------

bronze_count = spark.table(FULLY_QUALIFIED_TABLE).count()
assert (
    bronze_count > 0
), f"Bronze table '{FULLY_QUALIFIED_TABLE}' is empty after ingestion!"
logger.info(f"Bronze gate passed — {bronze_count:,} rows present.")

# COMMAND ----------

# Surface metrics for downstream tasks / Airflow XCom via DBFS file
dbutils.notebook.exit(
    str(
        {
            "table": FULLY_QUALIFIED_TABLE,
            "row_count": bronze_count,
            "load_mode": LOAD_MODE,
            "completed_at": datetime.utcnow().isoformat(),
        }
    )
)
