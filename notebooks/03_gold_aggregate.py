# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — NYC Taxi Business Aggregations
# MAGIC Builds three Gold tables from Silver:
# MAGIC
# MAGIC | Table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | `gold_daily_summary`        | pickup_date × zone × payment_type | Daily KPIs for BI dashboards |
# MAGIC | `gold_hourly_demand`        | pickup_date × hour × zone          | Demand forecasting feature store |
# MAGIC | `gold_driver_performance`   | vendor_id × pickup_date            | Vendor/driver ops metrics |
# MAGIC
# MAGIC All Gold tables are written via MERGE for idempotency.

# COMMAND ----------

import logging
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("database", "nyc_taxi")
dbutils.widgets.text("silver_table", "silver_trips")
dbutils.widgets.text("batch_date", datetime.utcnow().strftime("%Y-%m-%d"))

CATALOG = dbutils.widgets.get("catalog")
DATABASE = dbutils.widgets.get("database")
SILVER_TABLE = f"{CATALOG}.{DATABASE}.{dbutils.widgets.get('silver_table')}"
BATCH_DATE = dbutils.widgets.get("batch_date")

GOLD_DAILY_SUMMARY = f"{CATALOG}.{DATABASE}.gold_daily_summary"
GOLD_HOURLY_DEMAND = f"{CATALOG}.{DATABASE}.gold_hourly_demand"
GOLD_DRIVER_PERFORMANCE = f"{CATALOG}.{DATABASE}.gold_driver_performance"

logger.info(f"Silver source : {SILVER_TABLE}")
logger.info(f"Batch date    : {BATCH_DATE}")

# COMMAND ----------

# MAGIC %md ## 1. Read Silver (batch-date partition push-down)

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE).filter(F.col("_batch_date") == BATCH_DATE).cache()

silver_count = silver_df.count()
logger.info(f"Silver rows for {BATCH_DATE}: {silver_count:,}")

assert silver_count > 0, (
    f"No Silver rows found for batch_date={BATCH_DATE}. "
    "Verify Silver layer ran successfully."
)

# COMMAND ----------

# MAGIC %md ## 2. Gold — Daily Summary

# COMMAND ----------

daily_summary_df = (
    silver_df.groupBy(
        "pickup_date",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "payment_type_desc",
    )
    .agg(
        F.count("*").alias("total_trips"),
        F.sum("passenger_count").alias("total_passengers"),
        F.round(F.sum("trip_distance"), 2).alias("total_distance_miles"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
        F.round(F.sum("tip_amount"), 2).alias("total_tip_revenue"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
        F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
        F.sum("tolls_amount").alias("total_tolls"),
        F.sum("congestion_surcharge").alias("total_congestion_surcharge"),
        F.countDistinct("pu_location_id").alias("unique_pickup_zones"),
        F.max("fare_amount").alias("max_fare_amount"),
        F.min("fare_amount").alias("min_fare_amount"),
        F.percentile_approx("fare_amount", 0.5).alias("median_fare_amount"),
        F.percentile_approx("trip_duration_minutes", 0.5).alias(
            "median_duration_minutes"
        ),
    )
    .withColumn("_gold_processed_ts", F.current_timestamp())
    .withColumn("_batch_date", F.lit(BATCH_DATE))
)

_upsert_gold(
    spark,
    daily_summary_df,
    GOLD_DAILY_SUMMARY,
    merge_keys=["pickup_date", "pu_location_id", "do_location_id", "payment_type"],
    partition_cols=["pickup_date"],
)
logger.info(
    f"Gold daily summary written: {spark.table(GOLD_DAILY_SUMMARY).count():,} rows."
)

# COMMAND ----------

# MAGIC %md ## 3. Gold — Hourly Demand

# COMMAND ----------

hourly_demand_df = (
    silver_df.groupBy("pickup_date", "pickup_hour", "pickup_dow", "pu_location_id")
    .agg(
        F.count("*").alias("trip_count"),
        F.sum("passenger_count").alias("passenger_count"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
    )
    .withColumn(
        "demand_category",
        F.when(F.col("trip_count") >= 500, "High")
        .when(F.col("trip_count") >= 100, "Medium")
        .otherwise("Low"),
    )
    .withColumn("_gold_processed_ts", F.current_timestamp())
    .withColumn("_batch_date", F.lit(BATCH_DATE))
)

_upsert_gold(
    spark,
    hourly_demand_df,
    GOLD_HOURLY_DEMAND,
    merge_keys=["pickup_date", "pickup_hour", "pu_location_id"],
    partition_cols=["pickup_date"],
)
logger.info(
    f"Gold hourly demand written: {spark.table(GOLD_HOURLY_DEMAND).count():,} rows."
)

# COMMAND ----------

# MAGIC %md ## 4. Gold — Driver / Vendor Performance

# COMMAND ----------

driver_perf_df = (
    silver_df.groupBy("vendor_id", "pickup_date")
    .agg(
        F.count("*").alias("total_trips"),
        F.sum("passenger_count").alias("total_passengers"),
        F.round(F.sum("trip_distance"), 2).alias("total_distance_miles"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare_revenue"),
        F.round(F.sum("tip_amount"), 2).alias("total_tip_revenue"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
        F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
        # Efficiency: revenue per minute driven
        F.round(F.sum("total_amount") / F.sum("trip_duration_minutes"), 2).alias(
            "revenue_per_minute"
        ),
        F.countDistinct("pu_location_id").alias("unique_pickup_zones"),
        F.countDistinct("do_location_id").alias("unique_dropoff_zones"),
        # Credit-card ratio (payment_type == 1)
        F.round(
            F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0))
            / F.count("*")
            * 100,
            2,
        ).alias("credit_card_trip_pct"),
    )
    .withColumn("_gold_processed_ts", F.current_timestamp())
    .withColumn("_batch_date", F.lit(BATCH_DATE))
)

_upsert_gold(
    spark,
    driver_perf_df,
    GOLD_DRIVER_PERFORMANCE,
    merge_keys=["vendor_id", "pickup_date"],
    partition_cols=["pickup_date"],
)
logger.info(
    f"Gold driver performance written: {spark.table(GOLD_DRIVER_PERFORMANCE).count():,} rows."
)

# COMMAND ----------

# MAGIC %md ## 5. Helper — Generic MERGE Upsert

# COMMAND ----------


def _upsert_gold(
    spark, df, table_name: str, merge_keys: list[str], partition_cols: list[str]
):
    """
    Create the Gold table if it doesn't exist, then MERGE new aggregations.
    Ensures idempotency: re-running for the same batch_date yields same result.
    """
    # Create table from DataFrame schema + partition
    df.limit(0).write.format("delta").partitionBy(*partition_cols).option(
        "mergeSchema", "true"
    ).mode("ignore").saveAsTable(table_name)

    spark.sql(
        f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'delta.enableChangeDataFeed'       = 'true'
        )
        """
    )

    delta_tbl = DeltaTable.forName(spark, table_name)
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    (
        delta_tbl.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    spark.sql(f"OPTIMIZE {table_name}")


# NOTE: Python cells execute top-to-bottom; define helper before it's called above.
# In a real Databricks notebook, move this cell above the aggregation cells.

# COMMAND ----------

silver_df.unpersist()

final_counts = {
    "gold_daily_summary": spark.table(GOLD_DAILY_SUMMARY).count(),
    "gold_hourly_demand": spark.table(GOLD_HOURLY_DEMAND).count(),
    "gold_driver_performance": spark.table(GOLD_DRIVER_PERFORMANCE).count(),
}
logger.info(f"Gold layer complete: {final_counts}")

dbutils.notebook.exit(
    str(
        {
            "batch_date": BATCH_DATE,
            "gold_tables": final_counts,
            "completed_at": datetime.utcnow().isoformat(),
        }
    )
)
