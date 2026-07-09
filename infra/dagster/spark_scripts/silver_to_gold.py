from dagster_pipes import open_dagster_pipes
import logging
import sys
from pyspark.sql import SparkSession, functions as F

def write_partitioned_table(df, table_name, partition_cols):
    writer = df.writeTo(table_name).tableProperty("write.distribution-mode", "hash")
    if df.sparkSession.catalog.tableExists(table_name):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(*partition_cols).create()

def process(spark, pipes, dataset_type: str, target_year: int, target_month: int, branch_name: str):
    logger = pipes.log
    if dataset_type == "zone":
        logger.info("Zone lookup does not require aggregation to Gold. Skipping.")
        pipes.report_asset_materialization(metadata={"STATUS": "SKIPPED_FOR_ZONE"})
        return

    logger.info(f"STARTING SILVER TO GOLD FOR {dataset_type.upper()} (Branch: {branch_name})")
    
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    
    # Filter exactly for the dataset_type being requested
    df = spark.table("nessie.silver.trips").filter(
        (F.col("Year") == target_year) & 
        (F.col("Month") == target_month) & 
        (F.col("trip_type") == dataset_type)
    )
    logger.info(f"Silver row count for {dataset_type}: {df.count()}")
    
    daily_trips = (
        df.groupBy("Year", "Month", "trip_date", "trip_type")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("trip_distance").alias("total_distance"),
              F.sum("total_amount").alias("total_revenue"),
              F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds")
          )
    )
    
    monthly_summary = (
        df.groupBy("Year", "Month", "trip_type")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("trip_distance").alias("total_distance"),
              F.sum("total_amount").alias("total_revenue"),
              F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds")
          )
    )
    
    df_zone = spark.table("nessie.silver.dim_location")
    revenue_by_zone_raw = (
        df.groupBy("Year", "Month", "trip_type", "pulocation_id")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("total_amount").alias("total_revenue"),
              F.sum("tip_amount").alias("total_tip"),
              F.sum("fare_amount").alias("total_fare"),
              F.avg("fare_amount").alias("avg_fare"),
              F.avg("tip_amount").alias("avg_tip")
          )
    )
    
    revenue_by_zone = (
        revenue_by_zone_raw.join(
            df_zone, 
            revenue_by_zone_raw.pulocation_id == df_zone.LocationID, 
            "left"
        )
        .select(
            "Year", "Month", "trip_type",
            "pulocation_id", 
            F.col("Zone").alias("ZoneName"), 
            "Borough", 
            "total_trips", 
            "total_revenue",
            'avg_fare',
            'avg_tip',
            F.when(F.col("total_fare") == 0, 0).otherwise((F.col("total_tip") / F.col("total_fare")) * 100).alias("tip_percentage")
        )
        .orderBy(F.desc("total_revenue"))
    )
    
    # ---------------------------------------------------------
    # Aggregation 4: Payment Type Summary
    # ---------------------------------------------------------
    payment_type_summary = (
        df.filter(F.col("payment_type").isNotNull())
          .groupBy("Year", "Month", "trip_type", "payment_type")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("total_amount").alias("total_revenue"),
              F.sum("tip_amount").alias("total_tips")
          )
    )
    
    write_partitioned_table(daily_trips, "nessie.gold.daily_trips", ["Year", "Month", "trip_type"])
    write_partitioned_table(monthly_summary, "nessie.gold.monthly_summary", ["Year", "Month", "trip_type"])
    write_partitioned_table(revenue_by_zone, "nessie.gold.revenue_by_zone", ["Year", "Month", "trip_type"])
    write_partitioned_table(payment_type_summary, "nessie.gold.payment_type_summary", ["Year", "Month", "trip_type"])
    
    logger.info(f"GOLD AGGREGATION completed for {dataset_type}!")
    
    pipes.report_asset_materialization(
        metadata={
            "BRANCH_NAME": branch_name,
            "DATASET_TYPE": dataset_type,
            "TARGET_PERIOD": f"{target_year}-{target_month:02d}"
        }
    )

if __name__ == "__main__":
    with open_dagster_pipes() as pipes:
        dataset_type = pipes.get_extra("dataset_type")
        target_year = pipes.get_extra("target_year")
        target_month = pipes.get_extra("target_month")
        branch_name = pipes.get_extra("branch_name")
        
        spark = SparkSession.builder.appName(f"spark_silver_to_gold_{dataset_type}").getOrCreate()
        process(spark, pipes, dataset_type, target_year, target_month, branch_name)
        spark.stop()
