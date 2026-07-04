from dagster_pipes import open_dagster_pipes
from pyspark.sql import SparkSession
import great_expectations as gx

def process(spark, logger, target_year: int, target_month: int, branch_name: str):
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
                f"- {res.expectation_config.expectation_type} (Column: '{res.expectation_config.kwargs.get('column', 'N/A')}')"
                for res in validation_result.results if not res.success
            ]
            error_details = "\n".join(failed_msgs)
            raise AssertionError(
                f"Data Quality Check FAILED for table '{table_name}'.\n"
                f"Failed Expectations:\n{error_details}"
            )
    
    logger.info("Starting Data Quality Check for SILVER layer...")
    gx_context = gx.get_context(mode="ephemeral")
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    

    logger.info("1. Checking table nessie.silver.dim_location")
    df_dim_loc = spark.table("nessie.silver.dim_location")
    validate_table_with_ge(gx_context, df_dim_loc, "dim_location", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="LocationID"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="LocationID"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Zone")
    ])


    logger.info(f"2. Checking table nessie.silver.cleaned_trips for {target_year}-{target_month:02d}")
    df_cleaned = spark.table("nessie.silver.cleaned_trips").filter(f"Year = {target_year} AND Month = {target_month}")
    validate_table_with_ge(gx_context, df_cleaned, "cleaned_trips", [
        gx.expectations.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=20),
        gx.expectations.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0, max_value=99999),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0, max_value=999999999),
        gx.expectations.ExpectColumnValuesToBeBetween(column="trip_duration_seconds", min_value=0, max_value=9999999)
    ])

    logger.info("All Silver tables passed Great Expectations validation.")


if __name__ == "__main__":
    with open_dagster_pipes() as pipes:
        target_year = pipes.get_extra("target_year")
        target_month = pipes.get_extra("target_month")
        branch_name = pipes.get_extra("branch_name")
        
        spark = SparkSession.builder.appName("spark_job").getOrCreate()
        process(spark, pipes.log, target_year, target_month, branch_name)
        spark.stop()
