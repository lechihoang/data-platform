import sys
import os
from typing import Dict
from dagster import (
    MonthlyPartitionsDefinition,
    asset, 
    Definitions, 
    AssetExecutionContext, 
    resource, 
    MaterializeResult,
    define_asset_job
)
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from spark_scripts import (
    bronze_to_silver, 
    silver_to_gold, 
    dq_check_silver, 
    dq_check_gold, 
    merge_branch
)

@resource(config_schema={
    "app_name": str,
    "master": str,
    "spark_config": dict
})
def spark_resource(context):
    builder = SparkSession.builder.appName(context.resource_config["app_name"]).master(context.resource_config["master"])
    
    for key, value in context.resource_config["spark_config"].items():
        builder = builder.config(key, value)
        
    spark_session = builder.getOrCreate()
    
    yield spark_session
    
    spark_session.stop()


monthly_partitions = MonthlyPartitionsDefinition(start_date="2024-01-01")

def get_run_context(context: AssetExecutionContext):
    spark = context.resources.spark
    start_time = context.partition_time_window.start
    target_year = start_time.year
    target_month = start_time.month
    branch_name = f"etl_run_{target_year}_{target_month:02d}"
    return spark, target_year, target_month, branch_name

@asset(partitions_def=monthly_partitions, required_resource_keys={"spark"}, description="Clean Taxi data (Bronze -> Silver) with PySpark")
def silver_cleaned_trips(context: AssetExecutionContext):
    spark, target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running Bronze to Silver directly in Dagster on branch: {branch_name}")
    bronze_to_silver.process(spark, context.log, target_year, target_month, branch_name)
    return MaterializeResult(metadata={"layer": "silver", "status": "processed", "branch": branch_name})

@asset(partitions_def=monthly_partitions, required_resource_keys={"spark"}, deps=[silver_cleaned_trips], description="Check Silver data quality with Great Expectations")
def dq_check_silver_data(context: AssetExecutionContext):
    spark, target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running DQ Check Silver directly in Dagster on branch: {branch_name}")
    dq_check_silver.process(spark, context.log, target_year, target_month, branch_name)
    return MaterializeResult(metadata={"layer": "silver", "dq_status": "passed"})

@asset(partitions_def=monthly_partitions, required_resource_keys={"spark"}, deps=[dq_check_silver_data], description="Aggregate revenue (Silver -> Gold) with PySpark")
def gold_aggregated_revenue(context: AssetExecutionContext):
    spark, target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running Silver to Gold directly in Dagster on branch: {branch_name}")
    silver_to_gold.process(spark, context.log, target_year, target_month, branch_name)
    return MaterializeResult(metadata={"layer": "gold", "status": "processed", "branch": branch_name})

@asset(partitions_def=monthly_partitions, required_resource_keys={"spark"}, deps=[gold_aggregated_revenue], description="Check Gold data quality with Great Expectations")
def dq_check_gold_data(context: AssetExecutionContext):
    spark, target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running DQ Check Gold directly in Dagster on branch: {branch_name}")
    dq_check_gold.process(spark, context.log, target_year, target_month, branch_name)
    return MaterializeResult(metadata={"layer": "gold", "dq_status": "passed"})

@asset(partitions_def=monthly_partitions, required_resource_keys={"spark"}, deps=[dq_check_gold_data], description="Merge ETL branch into Main branch on Nessie")
def merge_nessie_branch(context: AssetExecutionContext):
    spark, target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running Merge Branch directly in Dagster from {branch_name} to main")
    merge_branch.process(spark, context.log, branch_name)
    return MaterializeResult(metadata={"branch": "main", "action": "merged", "from_branch": branch_name})

monthly_job = define_asset_job(
    name="monthly_taxi_pipeline",
    partitions_def=monthly_partitions,
    selection=[
        silver_cleaned_trips, 
        dq_check_silver_data, 
        gold_aggregated_revenue, 
        dq_check_gold_data, 
        merge_nessie_branch
    ]
)

defs = Definitions(
    assets=[
        silver_cleaned_trips, 
        dq_check_silver_data, 
        gold_aggregated_revenue, 
        dq_check_gold_data, 
        merge_nessie_branch
    ],
    jobs=[monthly_job],
    resources={
        "spark": spark_resource.configured({
            "app_name": "taxi_data_pipeline",
            "master": "spark://spark-master:7077",
            "spark_config": {
                "spark.driver.memory": "1g",
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.host": "dagster",
                "spark.driver.extraJavaOptions": "-Duser.dir=/tmp",
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": "file:/opt/spark-events",
                "spark.history.fs.logDirectory": "file:/opt/spark-events",
                "spark.ui.showConsoleProgress": "true"
            }
        })
    }
)
