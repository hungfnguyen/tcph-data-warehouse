from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)

class BaseTransformer(ABC):
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_name = self.__class__.__name__.replace("Transformer", "").lower()
    
    def read_bronze(self) -> DataFrame:
        # Read all batches from simple path
        path = f"s3a://bronze/{self.table_name}/"
        logger.info(f"Reading {self.table_name} from {path}")
        return self.spark.read.parquet(path)
    
    def write_silver(self, df: DataFrame):
        # Write single file, overwrite
        path = f"s3a://silver/{self.table_name}.parquet"
        logger.info(f"Writing {self.table_name} to {path}")
        
        df.coalesce(1).write.mode("overwrite").parquet(path)
        logger.info(f"Written {df.count():,} rows")
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass
    
    def run(self):
        logger.info(f"Starting {self.table_name} transformation")
        
        df = self.read_bronze()
        df_transformed = self.transform(df)
        self.write_silver(df_transformed)
        
        logger.info(f"Completed {self.table_name} transformation")