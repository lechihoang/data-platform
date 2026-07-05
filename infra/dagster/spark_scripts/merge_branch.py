from dagster_pipes import open_dagster_pipes
import logging
from pyspark.sql import SparkSession

def process(spark, pipes, branch_name: str):
    logger = pipes.log
    logger.info(f"STARTING MERGE: {branch_name} -> main")

    # MERGE BRANCH tạo merge commit trên main, giữ đầy đủ lịch sử Nessie.
    # Khác với ASSIGN BRANCH (chỉ dời pointer, mất lịch sử nếu chạy song song).
    spark.sql(f"MERGE BRANCH {branch_name} INTO main IN nessie")
    logger.info(f"Successfully merged {branch_name} into main")

    logger.info("MERGE completed successfully!")
    
    pipes.report_asset_materialization(
        metadata={
            "merged_branch": branch_name,
            "target_branch": "main",
            "execution_location": "Spark Cluster"
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes() as pipes:
        branch_name = pipes.get_extra("branch_name")
        
        spark = SparkSession.builder.appName("merge_branch").getOrCreate()
        process(spark, pipes, branch_name)
        spark.stop()
