import time
import logging
import sys
from pyspark.sql import SparkSession, functions as F

def process(spark, logger, target_year: int, target_month: int, branch_name: str):
    
    logger.info(
        "Spark app started: app_id=%s, eventLog.enabled=%s, eventLog.dir=%s",
        spark.sparkContext.applicationId,
        spark.conf.get("spark.eventLog.enabled"),
        spark.conf.get("spark.eventLog.dir"),
    )
    
    logger.info("STARTING BRONZE TO SILVER (Branch: %s)", branch_name)
    
    logger.info(f"Checkout branch: {branch_name}")
    spark.sql("USE REFERENCE main IN nessie")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN nessie FROM main")
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    
    logger.info("Creating namespace: nessie.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
    logger.info("Reading from bronze: nessie.bronze.yellow_tripdata")
    df = spark.read.parquet(f"s3a://lakehouse/bronze/yellow_tripdata_{target_year}-{target_month:02d}.parquet")
    logger.info(f"Raw row count: {df.count()}")
    
    df_clean = (
        df.filter(F.col("passenger_count") > 0)
          .filter(F.col("trip_distance") > 0.0)
          .filter(F.col("total_amount") >= 0.0)
          .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
          .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"])
    )
    
    df_enriched = (
        df_clean.withColumn("Year", F.year(F.col("tpep_pickup_datetime")))
                .withColumn("Month", F.month(F.col("tpep_pickup_datetime")))
                .withColumn("trip_date", F.to_date(F.col("tpep_pickup_datetime")))
                .withColumn("trip_duration_seconds", 
                            F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
                .withColumn("tip_percentage", 
                            F.when(F.col("total_amount") > 0, (F.col("tip_amount") / F.col("total_amount")) * 100).otherwise(0.0))
                .withColumn("payment_type_name",
                            F.when(F.col("payment_type") == 1, "Credit card")
                            .when(F.col("payment_type") == 2, "Cash")
                            .when(F.col("payment_type") == 3, "No charge")
                            .when(F.col("payment_type") == 4, "Dispute")
                            .when(F.col("payment_type") == 5, "Unknown")
                            .when(F.col("payment_type") == 6, "Voided trip")
                            .otherwise("Other"))
    )
    
    
    df_enriched = (
        df_enriched.filter(F.col("Year") == target_year)
                   .filter(F.col("Month") == target_month)
                   .filter(F.col("trip_duration_seconds") < 86400)
    )
    
    logger.info("Row count after cleaning: %d", df_enriched.count())
    

    df_enriched.write \
        .format("iceberg") \
        .mode("overwrite") \
        .option("replace-where", f"Year = {target_year} AND Month = {target_month}") \
        .saveAsTable("nessie.silver.cleaned_trips")
    
    logger.info("Saved data to nessie.silver.cleaned_trips")

    logger.info("Reading taxi zone lookup from bronze: s3a://lakehouse/bronze/taxi_zone_lookup.csv")
    df_zone = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv("s3a://lakehouse/bronze/taxi_zone_lookup.csv")
             .select(
                 F.col("LocationID").cast("int").alias("LocationID"),
                 F.col("Borough"),
                 F.col("Zone"),
                 F.col("service_zone"),
             )
             .dropDuplicates(["LocationID"])
    )
    logger.info("Zone lookup row count: %d", df_zone.count())

    df_zone.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("nessie.silver.dim_location")

    logger.info("Successfully wrote nessie.silver.dim_location")
    
    logger.info(f"Branch {branch_name} has been updated.")
    logger.info(f"To merge: spark.sql('ASSIGN BRANCH main TO {branch_name} IN nessie')")
    
    logger.info("BRONZE TO SILVER completed successfully!")
