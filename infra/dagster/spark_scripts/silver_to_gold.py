from dagster_pipes import open_dagster_pipes
import time
import logging
import sys
from pyspark.sql import SparkSession, functions as F
import datetime


def write_gold_table(spark, df, table_name, target_year, target_month):
    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .option("replace-where", f"Year = {target_year} AND Month = {target_month}") \
        .saveAsTable(table_name)

def process(spark, pipes, target_year: int, target_month: int, branch_name: str):
    logger = pipes.log
    
    logger.info(
        "Spark app started: app_id=%s, eventLog.enabled=%s, eventLog.dir=%s",
        spark.sparkContext.applicationId,
        spark.conf.get("spark.eventLog.enabled"),
        spark.conf.get("spark.eventLog.dir"),
    )
    
    logger.info(f"STARTING SILVER TO GOLD (Branch: {branch_name}, Partition: {target_year}-{target_month:02d})")
    
    logger.info(f"Checkout branch: {branch_name}")
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    
    logger.info("Creating namespace: nessie.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    
    
    logger.info("Reading from silver: nessie.silver.cleaned_trips")
    df = spark.table("nessie.silver.cleaned_trips").filter((F.col("Year") == target_year) & (F.col("Month") == target_month))
    logger.info(f"Silver row count: {df.count()}")
    
    daily_trips = (
        df.groupBy("Year", "Month", "trip_date")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("trip_distance").alias("total_distance"),
              F.sum("total_amount").alias("total_revenue"),
              F.avg("passenger_count").alias("avg_passengers"),
              F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
              F.avg("tip_percentage").alias("avg_tip_percentage")
          )
    )
    logger.info("Created daily_trips aggregate")
    

    
    monthly_summary = (
        df.groupBy("Year", "Month")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("trip_distance").alias("total_distance"),
              F.sum("total_amount").alias("total_revenue"),
              F.avg("passenger_count").alias("avg_passengers"),
              F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds")
          )
    )
    logger.info("Created monthly_summary aggregate")
    
    df_zone = spark.table("nessie.silver.dim_location")
    
    revenue_by_zone_raw = (
        df.groupBy("Year", "Month", "PULocationID")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("total_amount").alias("total_revenue"),
              F.avg("trip_distance").alias("avg_trip_distance")
          )
    )
    
    revenue_by_zone = (
        revenue_by_zone_raw.join(
            df_zone, 
            revenue_by_zone_raw.PULocationID == df_zone.LocationID, 
            "left"
        )
        .select(
            "Year", "Month",
            "PULocationID", 
            F.col("Zone").alias("ZoneName"), 
            "Borough", 
            "total_trips", 
            "total_revenue", 
            "avg_trip_distance"
        )
        .orderBy(F.desc("total_revenue"))
    )
    logger.info("Created revenue_by_zone aggregate with Zone Names")
    
    payment_type_summary = (
        df.groupBy("Year", "Month", F.col("payment_type_name").alias("payment_type"))
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("total_amount").alias("total_revenue"),
              F.avg("tip_percentage").alias("avg_tip_percentage")
          )
    )
    logger.info("Created payment_type_summary aggregate using mapped names from Silver")
    

    logger.info("Writing daily_trips to gold: nessie.gold.daily_trips")
    write_gold_table(spark, daily_trips, "nessie.gold.daily_trips", target_year, target_month)
    
    logger.info("Writing monthly_summary to gold: nessie.gold.monthly_summary")
    write_gold_table(spark, monthly_summary, "nessie.gold.monthly_summary", target_year, target_month)
    
    logger.info("Writing revenue_by_zone to gold: nessie.gold.revenue_by_zone")
    write_gold_table(spark, revenue_by_zone, "nessie.gold.revenue_by_zone", target_year, target_month)
    
    logger.info("Writing payment_type_summary to gold: nessie.gold.payment_type_summary")
    write_gold_table(spark, payment_type_summary, "nessie.gold.payment_type_summary", target_year, target_month)


    logger.info("SILVER TO GOLD completed successfully!")
    
    pipes.report_asset_materialization(
        metadata={
            "BRANCH NAME": branch_name,
            "TARGET PERIOD": f"{target_year}-{target_month:02d}"
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes() as pipes:
        target_year = pipes.get_extra("target_year")
        target_month = pipes.get_extra("target_month")
        branch_name = pipes.get_extra("branch_name")
        
        spark = SparkSession.builder.appName("spark_job").getOrCreate()
        process(spark, pipes, target_year, target_month, branch_name)
        spark.stop()
