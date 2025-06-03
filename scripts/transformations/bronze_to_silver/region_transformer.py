from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim
from scripts.transformations.utils.base_transformer import BaseTransformer


class RegionTransformer(BaseTransformer):
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df \
            .drop("_extracted_at", "_source", "_table") \
            .withColumn("r_name", trim(col("r_name"))) \
            .withColumn("r_comment", trim(col("r_comment")))