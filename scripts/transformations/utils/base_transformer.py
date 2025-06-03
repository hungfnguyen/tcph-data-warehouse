from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_name = self.__class__.__name__.replace("Transformer", "").lower()
        
    def read_bronze(self) -> DataFrame:
        path = f"s3a://bronze/tpch/{self.table_name}"
        logger.info(f"Reading {self.table_name} from {path}")
        return self.spark.read.parquet(path)
    
    def write_silver(self, df: DataFrame):
        path = f"s3a://silver/tpch/{self.table_name}"
        logger.info(f"Writing {self.table_name} to {path}")
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(path)
            
        # Log statistics
        count = df.count()
        logger.info(f"Written {count:,} rows to silver/{self.table_name}")
    
    def add_metadata(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import current_timestamp, lit
        
        return df \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processing_layer", lit("silver"))
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass
    
    def run(self):
        logger.info(f"Starting {self.table_name} transformation")
        
        # Read
        df = self.read_bronze()
        
        # Transform
        df_transformed = self.transform(df)
        
        # Add metadata
        df_final = self.add_metadata(df_transformed)
        
        # Write
        self.write_silver(df_final)
        
        logger.info(f"Completed {self.table_name} transformation")