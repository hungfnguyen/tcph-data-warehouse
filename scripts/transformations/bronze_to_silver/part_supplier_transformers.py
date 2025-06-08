from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when
from scripts.transformations.utils.base_transformer import BaseTransformer


class PartTransformer(BaseTransformer):
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df \
            .drop("_extracted_at", "_source", "_table") \
            .withColumn("p_name", trim(col("p_name"))) \
            .withColumn("p_mfgr", trim(col("p_mfgr"))) \
            .withColumn("p_brand", trim(col("p_brand"))) \
            .withColumn("p_type", trim(col("p_type"))) \
            .withColumn("p_container", trim(col("p_container"))) \
            .withColumn("p_size_category",
                       when(col("p_size") < 10, "SMALL")
                       .when(col("p_size") < 30, "MEDIUM")
                       .otherwise("LARGE")) \
            .filter(col("p_retailprice") > 0) \
            .repartition(4)


class PartsuppTransformer(BaseTransformer):
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df \
            .drop("_extracted_at", "_source", "_table") \
            .filter(col("ps_availqty") >= 0) \
            .filter(col("ps_supplycost") > 0) \
            .repartition(8)


class SupplierTransformer(BaseTransformer):
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df \
            .drop("_extracted_at", "_source", "_table") \
            .withColumn("s_name", trim(col("s_name"))) \
            .withColumn("s_address", trim(col("s_address"))) \
            .withColumn("s_phone", trim(col("s_phone"))) \
            .withColumn("s_acctbal_status",
                       when(col("s_acctbal") < 0, "NEGATIVE")
                       .otherwise("POSITIVE")) \
            .repartition(2)