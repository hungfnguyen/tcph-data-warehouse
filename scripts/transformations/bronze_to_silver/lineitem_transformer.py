from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, datediff, trim
from scripts.transformations.utils.base_transformer import BaseTransformer


class LineitemTransformer(BaseTransformer):
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df \
            .drop("_extracted_at", "_source", "_table") \
            .withColumn("l_returnflag", trim(col("l_returnflag"))) \
            .withColumn("l_linestatus", trim(col("l_linestatus"))) \
            .withColumn("l_shipinstruct", trim(col("l_shipinstruct"))) \
            .withColumn("l_shipmode", trim(col("l_shipmode"))) \
            .withColumn("l_net_price", 
                       col("l_extendedprice") * (1 - col("l_discount"))) \
            .withColumn("l_gross_price", 
                       col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))) \
            .withColumn("l_ship_year", year(col("l_shipdate"))) \
            .withColumn("l_ship_delay", 
                       datediff(col("l_shipdate"), col("l_commitdate"))) \
            .filter(col("l_quantity") > 0) \
            .repartition(50, "l_ship_year")