from abc import ABC, abstractmethod
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame, functions as F
import sys
import logging
from dagster_pipes import open_dagster_pipes

class AbstractProcessor(ABC):
    """Abstract Base Class for all data processors."""
    def __init__(self, spark: SparkSession, logger: logging.Logger, params: Dict[str, Any]):
        self.spark = spark
        self.logger = logger
        self.params = params

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass

    def execute(self) -> int:
        """Template method defining the skeleton of the ETL pipeline."""
        df = self.read()
        self.logger.info(f"Read {df.count()} rows from source")
        
        df_transformed = self.transform(df)
        final_count = df_transformed.count()
        self.logger.info(f"Transformed to {final_count} rows")
        
        self.write(df_transformed)
        return final_count


class TripProcessor(AbstractProcessor):
    """Base class specific to Trip datasets handling common transformation logic."""
    STANDARD_TYPES = {
        "trip_type": "string",
        "vendor_id": "string",
        "pickup_datetime": "timestamp",
        "dropoff_datetime": "timestamp",
        "pulocation_id": "integer",
        "dolocation_id": "integer",
        "passenger_count": "integer",
        "trip_distance": "double",
        "fare_amount": "double",
        "tip_amount": "double",
        "total_amount": "double",
        "payment_type": "integer",
        "tolls_amount": "double",
        "bcf": "double",
        "sales_tax": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double",
        "cbd_congestion_fee": "double"
    }

    def __init__(self, spark: SparkSession, logger: logging.Logger, params: Dict[str, Any], trip_type: str, file_prefix: str):
        super().__init__(spark, logger, params)
        self.trip_type = trip_type
        self.file_prefix = file_prefix

    def read(self) -> DataFrame:
        target_year = self.params.get("target_year")
        target_month = self.params.get("target_month")
        path = f"s3a://lakehouse/bronze/{self.file_prefix}_{target_year}-{target_month:02d}.parquet"
        self.logger.info(f"Reading from {path}")
        return self.spark.read.parquet(path)

    @abstractmethod
    def standardize_schema(self, df: DataFrame) -> DataFrame:
        """Hook method: Child classes define their specific schema renaming logic here."""
        pass

    def transform(self, df: DataFrame) -> DataFrame:
        # 1. Subclass-specific schema mapping
        df_std = self.standardize_schema(df)
        
        # 2. Add partition column
        df_std = df_std.withColumn("trip_type", F.lit(self.trip_type))
        
        # 3. Apply strict standard schema casting (automatically fills missing with Null)
        cols_to_select = []
        for col_name, type_str in self.STANDARD_TYPES.items():
            if col_name in df_std.columns:
                cols_to_select.append(F.col(col_name).cast(type_str).alias(col_name))
            else:
                cols_to_select.append(F.lit(None).cast(type_str).alias(col_name))
        
        df_std = df_std.select(*cols_to_select)
        
        # 4. Global cleaning rules for all trip data
        df_clean = (
            df_std
            .dropna(subset=["pickup_datetime", "dropoff_datetime", "pulocation_id", "dolocation_id"])
            .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
            .filter((F.col("fare_amount") >= 0) | F.col("fare_amount").isNull())
            .filter((F.col("total_amount") >= 0) | F.col("total_amount").isNull())
            .filter((F.col("passenger_count") > 0) | F.col("passenger_count").isNull())
            .filter((F.col("passenger_count") <= 9) | F.col("passenger_count").isNull())
            .filter((F.col("trip_distance") > 0) | F.col("trip_distance").isNull())
            .filter((F.col("trip_distance") < 150) | F.col("trip_distance").isNull())
            .fillna({"payment_type": 5, "cbd_congestion_fee": 0.0})
            .dropDuplicates()
        )
        
        # 5. Temporal Enrichment
        df_enriched = (
            df_clean
            .withColumn("Year", F.year(F.col("pickup_datetime")))
            .withColumn("Month", F.month(F.col("pickup_datetime")))
            .withColumn("trip_date", F.to_date(F.col("pickup_datetime")))
            .withColumn("trip_duration_seconds", 
                        F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
        )
        
        # 6. Target Partition Filtering
        target_year = self.params.get("target_year")
        target_month = self.params.get("target_month")
        
        df_final = (
            df_enriched
            .filter(F.col("Year") == target_year)
            .filter(F.col("Month") == target_month)
            .filter(F.col("trip_duration_seconds") < 86400) 
        )
        
        return df_final

    def write(self, df: DataFrame) -> None:
        table_name = "nessie.silver.trips"
        partition_cols = ["Year", "Month", "trip_type"]
        writer = df.writeTo(table_name).tableProperty("write.distribution-mode", "hash")
        
        if self.spark.catalog.tableExists(table_name):
            writer.overwritePartitions()
        else:
            writer.partitionedBy(*partition_cols).create()
            
        self.logger.info(f"Wrote data to Iceberg table {table_name}")


class YellowTripProcessor(TripProcessor):
    def __init__(self, spark: SparkSession, logger: logging.Logger, params: Dict[str, Any]):
        super().__init__(spark, logger, params, trip_type="yellow", file_prefix="yellow_tripdata")

    def standardize_schema(self, df: DataFrame) -> DataFrame:
        mapping = {
            "VendorID": "vendor_id",
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "PULocationID": "pulocation_id",
            "DOLocationID": "dolocation_id"
        }
        for src, tgt in mapping.items():
            if src in df.columns:
                df = df.withColumnRenamed(src, tgt)
        return df


class GreenTripProcessor(TripProcessor):
    def __init__(self, spark: SparkSession, logger: logging.Logger, params: Dict[str, Any]):
        super().__init__(spark, logger, params, trip_type="green", file_prefix="green_tripdata")

    def standardize_schema(self, df: DataFrame) -> DataFrame:
        mapping = {
            "VendorID": "vendor_id",
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
            "PULocationID": "pulocation_id",
            "DOLocationID": "dolocation_id"
        }
        for src, tgt in mapping.items():
            if src in df.columns:
                df = df.withColumnRenamed(src, tgt)
        return df


class FHVTripProcessor(TripProcessor):
    def __init__(self, spark: SparkSession, logger: logging.Logger, params: Dict[str, Any]):
        super().__init__(spark, logger, params, trip_type="fhv", file_prefix="fhv_tripdata")

    def standardize_schema(self, df: DataFrame) -> DataFrame:
        mapping = {
            "dispatching_base_num": "vendor_id",
            "pickup_datetime": "pickup_datetime",
            "dropOff_datetime": "dropoff_datetime",
            "PUlocationID": "pulocation_id",
            "DOlocationID": "dolocation_id"
        }
        for src, tgt in mapping.items():
            if src in df.columns:
                df = df.withColumnRenamed(src, tgt)
        return df


class HVFHVTripProcessor(TripProcessor):
    def __init__(self, spark: SparkSession, logger: logging.Logger, params: Dict[str, Any]):
        super().__init__(spark, logger, params, trip_type="hvfhv", file_prefix="fhvhv_tripdata")

    def standardize_schema(self, df: DataFrame) -> DataFrame:
        # HVFHV has detailed fare fields that need to be aggregated into total_amount
        df = df.withColumn("total_amount", 
            F.coalesce(F.col("base_passenger_fare"), F.lit(0.0)) + 
            F.coalesce(F.col("tolls"), F.lit(0.0)) + 
            F.coalesce(F.col("bcf"), F.lit(0.0)) + 
            F.coalesce(F.col("sales_tax"), F.lit(0.0)) + 
            F.coalesce(F.col("congestion_surcharge"), F.lit(0.0)) + 
            F.coalesce(F.col("tips"), F.lit(0.0))
        )
        
        mapping = {
            "hvfhs_license_num": "vendor_id",
            "pickup_datetime": "pickup_datetime",
            "dropoff_datetime": "dropoff_datetime",
            "PULocationID": "pulocation_id",
            "DOLocationID": "dolocation_id",
            "trip_miles": "trip_distance",
            "base_passenger_fare": "fare_amount",
            "tips": "tip_amount"
        }
        for src, tgt in mapping.items():
            if src in df.columns:
                df = df.withColumnRenamed(src, tgt)
        return df


class ZoneLookupProcessor(AbstractProcessor):
    """Processor dedicated to the Zone Lookup Dimension."""
    def read(self) -> DataFrame:
        path = "s3a://lakehouse/bronze/taxi_zone_lookup.csv"
        self.logger.info(f"Reading from {path}")
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    def transform(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("LocationID").cast("int").alias("LocationID"),
            F.col("Borough"),
            F.col("Zone"),
            F.col("service_zone")
        ).dropDuplicates(["LocationID"])

    def write(self, df: DataFrame) -> None:
        table_name = "nessie.silver.dim_location"
        df.writeTo(table_name).createOrReplace()
        self.logger.info(f"Wrote dimension to {table_name}")


class ProcessorFactory:
    """Factory to instantiate the appropriate processor based on Dagster parameters."""
    @staticmethod
    def get_processor(dataset_type: str, spark: SparkSession, logger: logging.Logger, params: Dict[str, Any]) -> AbstractProcessor:
        processors = {
            "yellow": YellowTripProcessor,
            "green": GreenTripProcessor,
            "fhv": FHVTripProcessor,
            "hvfhv": HVFHVTripProcessor,
            "zone": ZoneLookupProcessor
        }
        processor_class = processors.get(dataset_type)
        if not processor_class:
            raise ValueError(f"Unsupported dataset_type: {dataset_type}")
        return processor_class(spark, logger, params)


def initialize_nessie_branch(spark: SparkSession, branch_name: str, logger: logging.Logger):
    logger.info("Initializing Nessie context and checking out branch")
    spark.sql("USE REFERENCE main IN nessie")

    # -------------------------------------------------------------------------
    # IMPORTANT NOTE: 
    # Only uncomment the 3 lines of code below during the FIRST RUN of the entire project 
    # (when the Nessie repo is completely empty, and the main branch has no commits yet) to force Nessie 
    # to generate an Initial Commit. After the first run, please comment them out again.
    # -------------------------------------------------------------------------
    # logger.info("Initializing Nessie commit history")
    # spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dummy_init")
    # spark.sql("DROP NAMESPACE IF EXISTS nessie.dummy_init")
    # -------------------------------------------------------------------------
    
    spark.sql(f"DROP BRANCH IF EXISTS {branch_name} IN nessie")
        
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN nessie FROM main")
    spark.sql(f"USE REFERENCE {branch_name} IN nessie")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")


def process(spark, pipes, dataset_type: str, target_year: int, target_month: int, branch_name: str):
    logger = pipes.log
    params = {
        "target_year": target_year,
        "target_month": target_month,
        "branch_name": branch_name
    }
    initialize_nessie_branch(spark, branch_name, logger)
    
    processor = ProcessorFactory.get_processor(dataset_type, spark, logger, params)
    processed_rows = processor.execute()
    
    logger.info(f"Successfully processed {processed_rows} rows for {dataset_type} to branch {branch_name}")
    
    pipes.report_asset_materialization(
        metadata={
            "BRANCH_NAME": branch_name,
            "DATASET_TYPE": dataset_type,
            "TARGET_PERIOD": f"{target_year}-{target_month:02d}" if target_year else "N/A",
            "PROCESSED_ROWS": processed_rows
        }
    )

if __name__ == "__main__":
    with open_dagster_pipes() as pipes:
        dataset_type = pipes.get_extra("dataset_type")
        target_year = pipes.get_extra("target_year")
        target_month = pipes.get_extra("target_month")
        branch_name = pipes.get_extra("branch_name")
        
        spark = SparkSession.builder.appName(f"spark_silver_{dataset_type}").getOrCreate()
        process(spark, pipes, dataset_type, target_year, target_month, branch_name)
        spark.stop()
