from pyspark.sql import SparkSession, functions as F

def write_partitioned_table(df, table_name, partition_cols):
    writer = df.writeTo(table_name)
    if df.sparkSession.catalog.tableExists(table_name):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(*partition_cols).create()

def build_facts(spark, logger, dataset_type, target_year, target_month):
    df = spark.table("nessie.silver.trips").filter(
        (F.col("Year") == target_year) &
        (F.col("Month") == target_month) &
        (F.col("trip_type") == dataset_type)
    ).cache()
    logger.info(f"Silver row count for {dataset_type}: {df.count()}")

    fact_daily_trips = (
        df.groupBy("Year", "Month", "trip_date", "trip_type")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("trip_distance").alias("total_distance"),
              F.sum("total_amount").alias("total_revenue"),
              F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
          )
    )

    fact_monthly_summary = (
        df.groupBy("Year", "Month", "trip_type")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("trip_distance").alias("total_distance"),
              F.sum("total_amount").alias("total_revenue"),
              F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
          )
    )

    fact_revenue_by_zone = (
        df.groupBy("Year", "Month", "trip_type", "pulocation_id")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("total_amount").alias("total_revenue"),
              F.sum("tip_amount").alias("total_tip"),
              F.sum("fare_amount").alias("total_fare"),
              F.avg("fare_amount").alias("avg_fare"),
              F.avg("tip_amount").alias("avg_tip"),
          )
          .withColumn(
              "tip_percentage",
              F.when(F.col("total_fare") == 0, 0)
               .otherwise((F.col("total_tip") / F.col("total_fare")) * 100),
          )
    )

    fact_payment_summary = (
        df.filter(F.col("payment_type").isNotNull())
          .groupBy("Year", "Month", "trip_type", "payment_type")
          .agg(
              F.count("*").alias("total_trips"),
              F.sum("total_amount").alias("total_revenue"),
              F.sum("tip_amount").alias("total_tips"),
          )
          .withColumnRenamed("payment_type", "payment_type_id")
    )

    write_partitioned_table(fact_daily_trips, "nessie.gold.fact_daily_trips", ["Year", "Month", "trip_type"])
    write_partitioned_table(fact_monthly_summary, "nessie.gold.fact_monthly_summary", ["Year", "Month", "trip_type"])
    write_partitioned_table(fact_revenue_by_zone, "nessie.gold.fact_revenue_by_zone", ["Year", "Month", "trip_type"])
    write_partitioned_table(fact_payment_summary, "nessie.gold.fact_payment_summary", ["Year", "Month", "trip_type"])

    df.unpersist()

def process(spark, logger, dataset_type: str, target_year: int, target_month: int, branch_name: str, asset_key: str = None):
    logger.info(f"STARTING SILVER TO GOLD FOR {dataset_type.upper()} (Branch: {branch_name})")

    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    build_facts(spark, logger, dataset_type, target_year, target_month)
    logger.info(f"GOLD AGGREGATION completed for {dataset_type}!")

    return {
            "BRANCH_NAME": branch_name,
            "DATASET_TYPE": dataset_type,
            "TARGET_PERIOD": f"{target_year}-{target_month:02d}",
        }
