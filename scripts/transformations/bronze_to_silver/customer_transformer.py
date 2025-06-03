import sys
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when
from scripts.transformations.utils.base_transformer import BaseTransformer


class CustomerTransformer(BaseTransformer):
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df \
            .drop("_extracted_at", "_source", "_table") \
            .withColumn("c_name", trim(col("c_name"))) \
            .withColumn("c_address", trim(col("c_address"))) \
            .withColumn("c_phone", trim(col("c_phone"))) \
            .withColumn("c_mktsegment", trim(col("c_mktsegment"))) \
            .withColumn("c_acctbal_status", 
                       when(col("c_acctbal") < 0, "NEGATIVE")
                       .when(col("c_acctbal") == 0, "ZERO")
                       .otherwise("POSITIVE")) \
            .repartition(4)