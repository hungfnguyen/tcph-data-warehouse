from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, quarter, trim
from scripts.transformations.utils.base_transformer import BaseTransformer


class OrdersTransformer(BaseTransformer):
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df \
            .drop("_extracted_at", "_source", "_table") \
            .withColumn("o_orderstatus", trim(col("o_orderstatus"))) \
            .withColumn("o_orderpriority", trim(col("o_orderpriority"))) \
            .withColumn("o_year", year(col("o_orderdate"))) \
            .withColumn("o_month", month(col("o_orderdate"))) \
            .withColumn("o_quarter", quarter(col("o_orderdate"))) \
            .filter(col("o_totalprice") > 0) \
            .repartition(20, "o_year")