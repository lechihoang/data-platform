import logging
from pyspark.sql import SparkSession

def process(spark, logger, branch_name: str):
    logger.info(f"STARTING MERGE: {branch_name} -> main")
    
    spark.sql(f"ASSIGN BRANCH main TO {branch_name} IN nessie")
    logger.info(f"Successfully merged {branch_name} into main")
    
    logger.info("MERGE completed successfully!")
