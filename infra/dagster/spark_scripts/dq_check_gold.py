from dagster_pipes import open_dagster_pipes
from pyspark.sql import SparkSession
import great_expectations as gx

def process(spark, pipes, target_year: int, target_month: int, branch_name: str):
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
                f"- {res.expectation_config.expectation_type} (Column: '{res.expectation_config.kwargs.get('column', 'N/A')}')"
                for res in validation_result.results if not res.success
            ]
            error_details = "\n".join(failed_msgs)
            raise AssertionError(
                f"Data Quality Check FAILED for table '{table_name}'.\n"
                f"Failed Expectations:\n{error_details}"
            )
    
    logger.info("Starting Data Quality Check for GOLD layer...")
    gx_context = gx.get_context(mode="ephemeral")
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    

    logger.info(f"1. Checking table nessie.gold.daily_trips for {target_year}-{target_month:02d}")
    df_daily = spark.table("nessie.gold.daily_trips").filter(f"Year = {target_year} AND Month = {target_month}")
    validate_table_with_ge(gx_context, df_daily, "daily_trips", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="trip_date"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0, max_value=999999999),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_trips", min_value=1, max_value=999999999)
    ])
    

    logger.info(f"2. Checking table nessie.gold.monthly_summary for {target_year}-{target_month:02d}")
    df_monthly = spark.table("nessie.gold.monthly_summary").filter(f"Year = {target_year} AND Month = {target_month}")
    validate_table_with_ge(gx_context, df_monthly, "monthly_summary", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Year"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="Month", min_value=1, max_value=12),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0, max_value=999999999)
    ])


    logger.info(f"3. Checking table nessie.gold.revenue_by_zone for {target_year}-{target_month:02d}")
    df_zone = spark.table("nessie.gold.revenue_by_zone").filter(f"Year = {target_year} AND Month = {target_month}")
    validate_table_with_ge(gx_context, df_zone, "revenue_by_zone", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="PULocationID"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0, max_value=999999999),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Borough"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="ZoneName")
    ])


    logger.info(f"4. Checking table nessie.gold.payment_type_summary for {target_year}-{target_month:02d}")
    df_payment = spark.table("nessie.gold.payment_type_summary").filter(f"Year = {target_year} AND Month = {target_month}")
    validate_table_with_ge(gx_context, df_payment, "payment_type_summary", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="payment_type"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0, max_value=999999999)
    ])

    logger.info("All Gold tables passed Great Expectations validation.")
    
    pipes.report_asset_materialization(
        metadata={
            "branch_name": branch_name,
            "target_period": f"{target_year}-{target_month:02d}",
            "execution_location": "Spark Cluster",
            "dq_status": "PASSED"
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
