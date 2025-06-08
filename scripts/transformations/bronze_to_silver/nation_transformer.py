from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim
from scripts.transformations.utils.base_transformer import BaseTransformer


class NationTransformer(BaseTransformer):
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df \
            .drop("_extracted_at", "_source", "_table") \
            .withColumn("n_name", trim(col("n_name"))) \
            .withColumn("n_comment", trim(col("n_comment")))