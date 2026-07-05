import sys
import os
from typing import Dict
from dagster import (
    MonthlyPartitionsDefinition,
    asset, 
    Definitions, 
    AssetExecutionContext, 
    MaterializeResult,
    define_asset_job,
    PipesSubprocessClient,
    file_relative_path
)

monthly_partitions = MonthlyPartitionsDefinition(start_date="2024-01-01")

def get_run_context(context: AssetExecutionContext):
    start_time = context.partition_time_window.start
    target_year = start_time.year
    target_month = start_time.month
    branch_name = f"etl_run_{target_year}_{target_month:02d}"
    return target_year, target_month, branch_name

def build_spark_cmd(script_name):
    return [
        "spark-submit",
        "--conf", "spark.driver.memory=1g",
        "--conf", "spark.executor.memory=1g",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.driver.host=dagster",
        "--conf", "spark.driver.extraJavaOptions=-Duser.dir=/tmp",
        file_relative_path(__file__, f"spark_scripts/{script_name}")
    ]

@asset(partitions_def=monthly_partitions, description="Clean Taxi data (Bronze -> Silver) with PySpark")
def silver_cleaned_trips(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running Bronze to Silver via Dagster Pipes on branch: {branch_name}")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("bronze_to_silver.py"), 
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name}
    ).get_materialize_result()

@asset(partitions_def=monthly_partitions, deps=[silver_cleaned_trips], description="Check Silver data quality with Great Expectations")
def dq_check_silver_data(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running DQ Check Silver via Dagster Pipes on branch: {branch_name}")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("dq_check_silver.py"), 
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name}
    ).get_materialize_result()

@asset(partitions_def=monthly_partitions, deps=[dq_check_silver_data], description="Aggregate revenue (Silver -> Gold) with PySpark")
def gold_aggregated_revenue(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running Silver to Gold via Dagster Pipes on branch: {branch_name}")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("silver_to_gold.py"), 
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name}
    ).get_materialize_result()

@asset(partitions_def=monthly_partitions, deps=[gold_aggregated_revenue], description="Check Gold data quality with Great Expectations")
def dq_check_gold_data(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running DQ Check Gold via Dagster Pipes on branch: {branch_name}")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("dq_check_gold.py"), 
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name}
    ).get_materialize_result()

@asset(partitions_def=monthly_partitions, deps=[dq_check_gold_data], description="Merge ETL branch into Main branch on Nessie")
def merge_nessie_branch(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context)
    context.log.info(f"Running Merge Branch via Dagster Pipes from {branch_name} to main")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("merge_branch.py"), 
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name}
    ).get_materialize_result()

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
        "pipes_subprocess_client": PipesSubprocessClient()
    }
)
