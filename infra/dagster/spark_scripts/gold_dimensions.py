from dagster_pipes import open_dagster_pipes
from pyspark.sql import SparkSession, functions as F

def write_partitioned_table(df, table_name, partition_cols):
    writer = df.writeTo(table_name)
    if df.sparkSession.catalog.tableExists(table_name):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(*partition_cols).create()

PAYMENT_TYPE_ROWS = [
    (0, "Flex Fare trip"),
    (1, "Credit Card"),
    (2, "Cash"),
    (3, "No Charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided Trip"),
]

def build_dimensions(spark, logger, target_year, target_month):
    logger.info("Building conformed dimensions in nessie.gold")

    # dim_location: conform silver.dim_location -> gold naming
    dim_location = spark.table("nessie.silver.dim_location").select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone_name"),
        F.col("service_zone"),
    )
    dim_location.writeTo("nessie.gold.dim_location").createOrReplace()
    logger.info("Wrote nessie.gold.dim_location")

    # dim_payment_type: static reference table
    dim_payment = spark.createDataFrame(PAYMENT_TYPE_ROWS, ["payment_type_id", "description"])
    dim_payment.writeTo("nessie.gold.dim_payment_type").createOrReplace()
    logger.info("Wrote nessie.gold.dim_payment_type")



def process(spark, pipes, target_year: int, target_month: int, branch_name: str):
    logger = pipes.log
    logger.info(f"STARTING GOLD DIMENSIONS (Branch: {branch_name})")

    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    build_dimensions(spark, logger, target_year, target_month)
    logger.info("GOLD DIMENSIONS completed!")

    pipes.report_asset_materialization(
        metadata={
            "BRANCH_NAME": branch_name,
            "TARGET_PERIOD": f"{target_year}-{target_month:02d}" if target_year else "N/A",
            "GOLD_OBJECTS": "dim_location, dim_payment_type",
        }
    )

if __name__ == "__main__":
    with open_dagster_pipes() as pipes:
        target_year = pipes.get_extra("target_year")
        target_month = pipes.get_extra("target_month")
        branch_name = pipes.get_extra("branch_name")

        spark = SparkSession.builder.appName("spark_gold_dimensions").getOrCreate()
        process(spark, pipes, target_year, target_month, branch_name)
        spark.stop()
