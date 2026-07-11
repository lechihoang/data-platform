import sys
import os
os.environ["HADOOP_USER_NAME"] = "dagster"
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

def get_run_context(context: AssetExecutionContext, dtype: str):
    if dtype != "zone":
        start_time = context.partition_time_window.start
        target_year = start_time.year
        target_month = start_time.month
        branch_name = f"etl_run_{dtype}_{target_year}_{target_month:02d}"
        return target_year, target_month, branch_name
    else:
        run_id = context.run_id[:8]
        branch_name = f"etl_run_{dtype}_{run_id}"
        return None, None, branch_name

def build_spark_cmd(script_name, driver_memory="2g", executor_memory="3g"):
    return [
        "spark-submit",
        "--conf", f"spark.driver.memory={driver_memory}",
        "--conf", f"spark.executor.memory={executor_memory}",
        "--conf", "spark.executor.cores=4",
        "--conf", "spark.sql.shuffle.partitions=10",
        "--conf", "spark.driver.host=dagster",
        "--conf", "spark.driver.extraJavaOptions=-Duser.dir=/tmp",
        file_relative_path(__file__, f"spark_scripts/{script_name}")
    ]

# ---------------------------------------------------------
# DYNAMIC ASSET GENERATION FOR EACH DATASET TYPE
# ---------------------------------------------------------
dataset_types = ["yellow", "green", "fhv", "hvfhv"]
all_assets = []

# 1. Zone Lookup
@asset(name="silver_zone", description="Process Zone Lookup")
def silver_zone(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context, "zone")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("bronze_to_silver.py"), 
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name, "dataset_type": "zone"}
    ).get_materialize_result()

@asset(name="dq_check_silver_zone", deps=[silver_zone], description="DQ Check Zone")
def dq_check_silver_zone(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context, "zone")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("dq_check_silver.py"), 
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name, "dataset_type": "zone"}
    ).get_materialize_result()

@asset(name="gold_dimensions", deps=[dq_check_silver_zone], description="Build shared conformed dimensions (dim_location, dim_payment_type)")
def gold_dimensions(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context, "zone")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("gold_dimensions.py", executor_memory="2g"),
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name, "dataset_type": "zone"}
    ).get_materialize_result()

@asset(name="merge_nessie_branch_zone", deps=[gold_dimensions], description="Merge Zone Branch")
def merge_nessie_branch_zone(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    target_year, target_month, branch_name = get_run_context(context, "zone")
    return pipes_subprocess_client.run(
        command=build_spark_cmd("merge_branch.py"),
        context=context,
        extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name}
    ).get_materialize_result()

all_assets.extend([silver_zone, dq_check_silver_zone, gold_dimensions, merge_nessie_branch_zone])

# 2. Fact Trips
def build_assets_for_type(dtype: str):
    
    @asset(partitions_def=monthly_partitions, name=f"silver_{dtype}", description=f"Bronze -> Silver ({dtype})")
    def _bronze_to_silver(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        return pipes_subprocess_client.run(
            command=build_spark_cmd("bronze_to_silver.py"), 
            context=context,
            extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name, "dataset_type": dtype}
        ).get_materialize_result()

    @asset(partitions_def=monthly_partitions, name=f"dq_check_silver_{dtype}", deps=[_bronze_to_silver], description=f"DQ Check Silver ({dtype})")
    def _dq_silver(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        return pipes_subprocess_client.run(
            command=build_spark_cmd("dq_check_silver.py"), 
            context=context,
            extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name, "dataset_type": dtype}
        ).get_materialize_result()
        
    @asset(partitions_def=monthly_partitions, name=f"gold_aggregates_{dtype}", deps=[_dq_silver], description=f"Silver -> Gold ({dtype})")
    def _silver_to_gold(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        return pipes_subprocess_client.run(
            command=build_spark_cmd("silver_to_gold.py"), 
            context=context,
            extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name, "dataset_type": dtype}
        ).get_materialize_result()

    @asset(partitions_def=monthly_partitions, name=f"dq_check_gold_{dtype}", deps=[_silver_to_gold], description=f"DQ Check Gold ({dtype})")
    def _dq_gold(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        return pipes_subprocess_client.run(
            command=build_spark_cmd("dq_check_gold.py"), 
            context=context,
            extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name, "dataset_type": dtype}
        ).get_materialize_result()

    @asset(partitions_def=monthly_partitions, name=f"merge_nessie_branch_{dtype}", deps=[_dq_gold], description=f"Merge {dtype} Branch")
    def _merge_branch(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        return pipes_subprocess_client.run(
            command=build_spark_cmd("merge_branch.py"), 
            context=context,
            extras={"target_year": target_year, "target_month": target_month, "branch_name": branch_name}
        ).get_materialize_result()

    return [_bronze_to_silver, _dq_silver, _silver_to_gold, _dq_gold, _merge_branch]

for t in dataset_types:
    all_assets.extend(build_assets_for_type(t))

# ---------------------------------------------------------
# JOB & DEFINITIONS
# ---------------------------------------------------------
zone_job = define_asset_job(
    name="zone_pipeline",
    selection=["silver_zone", "dq_check_silver_zone", "gold_dimensions", "merge_nessie_branch_zone"]
)

yellow_job = define_asset_job(
    name="yellow_pipeline",
    partitions_def=monthly_partitions,
    selection=["silver_yellow", "dq_check_silver_yellow", "gold_aggregates_yellow", "dq_check_gold_yellow", "merge_nessie_branch_yellow"]
)

green_job = define_asset_job(
    name="green_pipeline",
    partitions_def=monthly_partitions,
    selection=["silver_green", "dq_check_silver_green", "gold_aggregates_green", "dq_check_gold_green", "merge_nessie_branch_green"]
)

fhv_job = define_asset_job(
    name="fhv_pipeline",
    partitions_def=monthly_partitions,
    selection=["silver_fhv", "dq_check_silver_fhv", "gold_aggregates_fhv", "dq_check_gold_fhv", "merge_nessie_branch_fhv"]
)

hvfhv_job = define_asset_job(
    name="hvfhv_pipeline",
    partitions_def=monthly_partitions,
    selection=["silver_hvfhv", "dq_check_silver_hvfhv", "gold_aggregates_hvfhv", "dq_check_gold_hvfhv", "merge_nessie_branch_hvfhv"]
)

defs = Definitions(
    assets=all_assets,
    jobs=[zone_job, yellow_job, green_job, fhv_job, hvfhv_job],
    resources={
        "pipes_subprocess_client": PipesSubprocessClient()
    }
)
