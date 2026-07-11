import logging
from pyspark.sql import SparkSession

def process(spark, logger, branch_name: str, asset_key: str = None):
    logger.info(f"STARTING MERGE: {branch_name} -> main")

    # MERGE BRANCH tạo merge commit trên main, giữ đầy đủ lịch sử Nessie.
    # Khác với ASSIGN BRANCH (chỉ dời pointer, mất lịch sử nếu chạy song song).
    spark.sql(f"MERGE BRANCH {branch_name} INTO main IN nessie")
    logger.info(f"Successfully merged {branch_name} into main")
    
    # DROP BRANCH để dọn dẹp rác catalog sau khi đã publish thành công
    spark.sql(f"DROP BRANCH {branch_name} IN nessie")
    logger.info(f"Successfully dropped branch {branch_name}")    
    return {
            "MERGED BRANCH": branch_name,
            "TARGET BRANCH": "main"
        }
