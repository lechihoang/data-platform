import sys
from dagster_pipes import open_dagster_pipes
from pyspark.sql import SparkSession
import great_expectations as gx

def process(spark, pipes, dataset_type: str, target_year: int, target_month: int, branch_name: str):
    logger = pipes.log
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
                f"- {res.expectation_config.type} (Column: '{res.expectation_config.kwargs.get('column', 'N/A')}')"
                for res in validation_result.results if not res.success
            ]
            error_details = "\n".join(failed_msgs)
            raise AssertionError(
                f"Data Quality Check FAILED for table '{table_name}'.\n"
                f"Failed Expectations:\n{error_details}"
            )
    
    logger.info(f"Starting Data Quality Check for SILVER layer - Type: {dataset_type.upper()}")
    gx_context = gx.get_context(mode="ephemeral")
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    
    if dataset_type == "zone":
        logger.info("Checking table nessie.silver.dim_location")
        df_dim_loc = spark.table("nessie.silver.dim_location")
        validate_table_with_ge(gx_context, df_dim_loc, "dim_location", [
            gx.expectations.ExpectColumnValuesToNotBeNull(column="LocationID"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="LocationID"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="Zone")
        ])
    else:
        logger.info(f"Checking table nessie.silver.trips for {dataset_type} ({target_year}-{target_month:02d})")
        df_trips = spark.table("nessie.silver.trips").filter(
            f"Year = {target_year} AND Month = {target_month} AND trip_type = '{dataset_type}'"
        )
        
        # Common checks
        validate_table_with_ge(gx_context, df_trips, f"trips_{dataset_type}_common", [
            gx.expectations.ExpectColumnValuesToNotBeNull(column="trip_type"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="dropoff_datetime"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="pulocation_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="dolocation_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="trip_duration_seconds", min_value=0, max_value=86400)
        ])
        
        # Fare checks
        if dataset_type in ['yellow', 'green', 'hvfhv']:
            validate_table_with_ge(gx_context, df_trips, f"trips_{dataset_type}_fares", [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="total_amount"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0, max_value=99999),
                gx.expectations.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=9, mostly=0.9),
                gx.expectations.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0, max_value=150, mostly=0.9)
            ])

    logger.info(f"Silver DQ validation passed for {dataset_type}.")
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
        
        spark = SparkSession.builder.appName(f"spark_dq_silver_{dataset_type}").getOrCreate()
        process(spark, pipes, dataset_type, target_year, target_month, branch_name)
        spark.stop()
