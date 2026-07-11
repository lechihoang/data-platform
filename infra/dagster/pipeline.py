import os
import sys

os.environ["HADOOP_USER_NAME"] = "dagster"

from dagster import Definitions  # noqa: E402
from dagster import (AssetExecutionContext, MaterializeResult,
                     MonthlyPartitionsDefinition, asset, define_asset_job,
                     in_process_executor)
from dagster_pyspark import PySparkResource  # noqa: E402

# Add the current directory to sys.path so we can import spark_scripts as a module
sys.path.append(os.path.dirname(__file__))

from spark_scripts import dq_check_gold  # noqa: E402
from spark_scripts import (bronze_to_silver, dq_check_silver, gold_dimensions,
                           merge_branch, silver_to_gold)

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


# ---------------------------------------------------------
# DYNAMIC ASSET GENERATION FOR EACH DATASET TYPE
# ---------------------------------------------------------
dataset_types = ["yellow", "green", "fhv", "hvfhv"]
all_assets = []


# ZONE
@asset(name="silver_zone", description="Bronze to Silver for Zone")
def silver_zone(context: AssetExecutionContext, pyspark: PySparkResource):
    target_year, target_month, branch_name = get_run_context(context, "zone")
    spark = pyspark.spark_session
    metadata = bronze_to_silver.process(
        spark,
        context.log,
        "zone",
        target_year,
        target_month,
        branch_name,
        "silver_zone",
    )
    return MaterializeResult(metadata=metadata)


@asset(
    name="dq_check_silver_zone", description="DQ Check Silver Zone", deps=[silver_zone]
)
def dq_check_silver_zone(context: AssetExecutionContext, pyspark: PySparkResource):
    target_year, target_month, branch_name = get_run_context(context, "zone")
    spark = pyspark.spark_session
    metadata = dq_check_silver.process(
        spark,
        context.log,
        "zone",
        target_year,
        target_month,
        branch_name,
        "dq_check_silver_zone",
    )
    return MaterializeResult(metadata=metadata)


@asset(
    name="gold_dimensions",
    description="Silver to Gold (Dimensions) for Zone",
    deps=[dq_check_silver_zone],
)
def gold_dimensions_asset(context: AssetExecutionContext, pyspark: PySparkResource):
    target_year, target_month, branch_name = get_run_context(context, "zone")
    spark = pyspark.spark_session
    metadata = gold_dimensions.process(
        spark, context.log, target_year, target_month, branch_name, "gold_dimensions"
    )
    return MaterializeResult(metadata=metadata)


@asset(
    name="merge_nessie_branch_zone",
    description="Merge Zone Branch to Main",
    deps=[gold_dimensions_asset],
)
def merge_nessie_branch_zone(context: AssetExecutionContext, pyspark: PySparkResource):
    target_year, target_month, branch_name = get_run_context(context, "zone")
    spark = pyspark.spark_session
    metadata = merge_branch.process(
        spark, context.log, branch_name, "merge_nessie_branch_zone"
    )
    return MaterializeResult(metadata=metadata)


all_assets.extend(
    [silver_zone, dq_check_silver_zone, gold_dimensions, merge_nessie_branch_zone]
)


# OTHER DATASETS
def build_assets_for_type(dtype: str):

    @asset(
        name=f"silver_{dtype}",
        partitions_def=monthly_partitions,
        description=f"Bronze to Silver for {dtype}",
    )
    def _silver(context: AssetExecutionContext, pyspark: PySparkResource):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        spark = pyspark.spark_session
        metadata = bronze_to_silver.process(
            spark,
            context.log,
            dtype,
            target_year,
            target_month,
            branch_name,
            f"silver_{dtype}",
        )
        return MaterializeResult(metadata=metadata)

    @asset(
        name=f"dq_check_silver_{dtype}",
        partitions_def=monthly_partitions,
        description=f"DQ Check Silver for {dtype}",
        deps=[_silver],
    )
    def _dq_silver(context: AssetExecutionContext, pyspark: PySparkResource):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        spark = pyspark.spark_session
        metadata = dq_check_silver.process(
            spark,
            context.log,
            dtype,
            target_year,
            target_month,
            branch_name,
            f"dq_check_silver_{dtype}",
        )
        return MaterializeResult(metadata=metadata)

    @asset(
        name=f"gold_aggregates_{dtype}",
        partitions_def=monthly_partitions,
        description=f"Silver to Gold for {dtype}",
        deps=[_dq_silver],
    )
    def _gold(context: AssetExecutionContext, pyspark: PySparkResource):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        spark = pyspark.spark_session
        metadata = silver_to_gold.process(
            spark,
            context.log,
            dtype,
            target_year,
            target_month,
            branch_name,
            f"gold_aggregates_{dtype}",
        )
        return MaterializeResult(metadata=metadata)

    @asset(
        name=f"dq_check_gold_{dtype}",
        partitions_def=monthly_partitions,
        description=f"DQ Check Gold for {dtype}",
        deps=[_gold],
    )
    def _dq_gold(context: AssetExecutionContext, pyspark: PySparkResource):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        spark = pyspark.spark_session
        metadata = dq_check_gold.process(
            spark,
            context.log,
            dtype,
            target_year,
            target_month,
            branch_name,
            f"dq_check_gold_{dtype}",
        )
        return MaterializeResult(metadata=metadata)

    @asset(
        name=f"merge_nessie_branch_{dtype}",
        partitions_def=monthly_partitions,
        description=f"Merge {dtype} Branch to Main",
        deps=[_dq_gold],
    )
    def _merge(context: AssetExecutionContext, pyspark: PySparkResource):
        target_year, target_month, branch_name = get_run_context(context, dtype)
        spark = pyspark.spark_session
        metadata = merge_branch.process(
            spark, context.log, branch_name, f"merge_nessie_branch_{dtype}"
        )
        return MaterializeResult(metadata=metadata)

    return [_silver, _dq_silver, _gold, _dq_gold, _merge]


for t in dataset_types:
    all_assets.extend(build_assets_for_type(t))

# ---------------------------------------------------------
# JOB & DEFINITIONS
# ---------------------------------------------------------
zone_job = define_asset_job(
    name="zone_pipeline",
    selection=[
        "silver_zone",
        "dq_check_silver_zone",
        "gold_dimensions",
        "merge_nessie_branch_zone",
    ],
)

yellow_job = define_asset_job(
    name="yellow_pipeline",
    partitions_def=monthly_partitions,
    selection=[
        "silver_yellow",
        "dq_check_silver_yellow",
        "gold_aggregates_yellow",
        "dq_check_gold_yellow",
        "merge_nessie_branch_yellow",
    ],
)

green_job = define_asset_job(
    name="green_pipeline",
    partitions_def=monthly_partitions,
    selection=[
        "silver_green",
        "dq_check_silver_green",
        "gold_aggregates_green",
        "dq_check_gold_green",
        "merge_nessie_branch_green",
    ],
)

fhv_job = define_asset_job(
    name="fhv_pipeline",
    partitions_def=monthly_partitions,
    selection=[
        "silver_fhv",
        "dq_check_silver_fhv",
        "gold_aggregates_fhv",
        "dq_check_gold_fhv",
        "merge_nessie_branch_fhv",
    ],
)

hvfhv_job = define_asset_job(
    name="hvfhv_pipeline",
    partitions_def=monthly_partitions,
    selection=[
        "silver_hvfhv",
        "dq_check_silver_hvfhv",
        "gold_aggregates_hvfhv",
        "dq_check_gold_hvfhv",
        "merge_nessie_branch_hvfhv",
    ],
)

pyspark_resource = PySparkResource(
    spark_config={"spark.app.name": "Dagster_PySpark_App"}
)

defs = Definitions(
    assets=all_assets,
    jobs=[zone_job, yellow_job, green_job, fhv_job, hvfhv_job],
    resources={"pyspark": pyspark_resource},
    executor=in_process_executor,
)
