from dagster_pipes import open_dagster_pipes
import time
import logging
import sys
from pyspark.sql import SparkSession, functions as F


def write_partitioned_table(df, table_name, partition_cols):
    writer = df.writeTo(table_name).tableProperty("write.distribution-mode", "hash")
    if df.sparkSession.catalog.tableExists(table_name):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(*partition_cols).create()


def process(spark, pipes, target_year: int, target_month: int, branch_name: str):
    logger = pipes.log
    
    logger.info(
        f"Spark app started: app_id={spark.sparkContext.applicationId}, eventLog.enabled={spark.conf.get('spark.eventLog.enabled')}, eventLog.dir={spark.conf.get('spark.eventLog.dir')}"
    )
    
    logger.info(f"STARTING BRONZE TO SILVER (Branch: {branch_name})")
    
    logger.info(f"Checkout branch: main")
    spark.sql("USE REFERENCE main IN nessie")
    
    # -------------------------------------------------------------------------
    # IMPORTANT NOTE: 
    # Only uncomment the 3 lines of code below during the FIRST RUN of the entire project 
    # (when the Nessie repo is completely empty, and the main branch has no commits yet) to force Nessie 
    # to generate an Initial Commit. After the first run, please comment them out again.
    # -------------------------------------------------------------------------
    logger.info("Initializing Nessie commit history")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dummy_init")
    spark.sql("DROP NAMESPACE IF EXISTS nessie.dummy_init")
    # -------------------------------------------------------------------------
    
    logger.info(f"Creating and checking out branch: {branch_name}")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN nessie FROM main")
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    
    logger.info(f"Creating namespace: nessie.silver on branch {branch_name}")
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
    
    final_row_count = df_enriched.count()
    logger.info(f"Row count after cleaning: {final_row_count}")
    

    write_partitioned_table(df_enriched, "nessie.silver.cleaned_trips", ["Year", "Month"])

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
    logger.info(f"Zone lookup row count: {df_zone.count()}")

    df_zone.writeTo("nessie.silver.dim_location").createOrReplace()

    logger.info("Successfully wrote nessie.silver.dim_location")
    
    logger.info(f"Branch {branch_name} has been updated.")
    logger.info(f"To merge: spark.sql('ASSIGN BRANCH main TO {branch_name} IN nessie')")
    
    logger.info("BRONZE TO SILVER completed successfully!")
    
    pipes.report_asset_materialization(
        metadata={
            "BRANCH NAME": branch_name,
            "PROCESSED ROWS": final_row_count,
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
