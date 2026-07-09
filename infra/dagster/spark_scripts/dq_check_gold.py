from dagster_pipes import open_dagster_pipes
from pyspark.sql import SparkSession
import great_expectations as gx

def process(spark, pipes, dataset_type: str, target_year: int, target_month: int, branch_name: str):
    logger = pipes.log
    if dataset_type == "zone":
        logger.info("Zone lookup does not require Gold DQ checking. Skipping.")
        pipes.report_asset_materialization(metadata={"STATUS": "SKIPPED_FOR_ZONE"})
        return

    def validate_table_with_ge(gx_context, df, table_name, expectations_list):
        data_source = gx_context.data_sources.add_spark(f"spark_source_{table_name}")
        data_asset = data_source.add_dataframe_asset(f"asset_{table_name}")
        batch_def = data_asset.add_batch_definition_whole_dataframe(f"batch_{table_name}")
        
        suite = gx_context.suites.add(gx.ExpectationSuite(name=f"suite_{table_name}"))
        for expectation in expectations_list:
            suite.add_expectation(expectation)
            
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})
        validation_result = batch.validate(suite)
        if not validation_result.success:
            failed_msgs = [
                f"- {getattr(res.expectation_config, 'expectation_type', getattr(res.expectation_config, 'type', 'Unknown'))} (Column: '{res.expectation_config.kwargs.get('column', 'N/A')}')"
                for res in validation_result.results if not res.success
            ]
            error_details = "\n".join(failed_msgs)
            raise AssertionError(
                f"Data Quality Check FAILED for GOLD table '{table_name}'.\n"
                f"Failed Expectations:\n{error_details}"
            )
    
    logger.info(f"Starting Data Quality Check for GOLD layer - Type: {dataset_type.upper()}")
    gx_context = gx.get_context(mode="ephemeral")
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    
    df_monthly = spark.table("nessie.gold.monthly_summary").filter(
        f"Year = {target_year} AND Month = {target_month} AND trip_type = '{dataset_type}'"
    )
    validate_table_with_ge(gx_context, df_monthly, f"monthly_summary_{dataset_type}", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="total_trips"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_trips", min_value=1, max_value=99999999),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="trip_type")
    ])

    df_revenue = spark.table("nessie.gold.revenue_by_zone").filter(
        f"Year = {target_year} AND Month = {target_month} AND trip_type = '{dataset_type}'"
    )
    revenue_expectations = [gx.expectations.ExpectColumnValuesToNotBeNull(column="pulocation_id")]
    if dataset_type != "fhv":
        revenue_expectations.append(gx.expectations.ExpectColumnValuesToNotBeNull(column="total_revenue"))
        
    validate_table_with_ge(gx_context, df_revenue, f"revenue_by_zone_{dataset_type}", revenue_expectations)

    logger.info(f"Gold DQ validation passed for {dataset_type}.")

    pipes.report_asset_materialization(
        metadata={
            "BRANCH_NAME": branch_name,
            "DATASET_TYPE": dataset_type,
            "TARGET_PERIOD": f"{target_year}-{target_month:02d}" if dataset_type != "zone" else "N/A",
            "DATA_QUALITY_STATUS": "PASSED"
        }
    )

if __name__ == "__main__":
    with open_dagster_pipes() as pipes:
        dataset_type = pipes.get_extra("dataset_type")
        target_year = pipes.get_extra("target_year")
        target_month = pipes.get_extra("target_month")
        branch_name = pipes.get_extra("branch_name")
        
        spark = SparkSession.builder.appName(f"spark_dq_gold_{dataset_type}").getOrCreate()
        process(spark, pipes, dataset_type, target_year, target_month, branch_name)
        spark.stop()
