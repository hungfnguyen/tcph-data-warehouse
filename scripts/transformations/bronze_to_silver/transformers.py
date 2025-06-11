from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when, year, month, quarter, datediff
from scripts.transformations.utils.base_transformer import BaseTransformer


class CustomerTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("c_name", trim(col("c_name"))) \
            .withColumn("c_address", trim(col("c_address"))) \
            .withColumn("c_phone", trim(col("c_phone"))) \
            .withColumn("c_mktsegment", trim(col("c_mktsegment"))) \
            .withColumn("c_acctbal_status", 
                when(col("c_acctbal") < 0, "NEGATIVE").otherwise("POSITIVE"))


class LineitemTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("l_returnflag", trim(col("l_returnflag"))) \
            .withColumn("l_linestatus", trim(col("l_linestatus"))) \
            .withColumn("l_shipmode", trim(col("l_shipmode"))) \
            .withColumn("l_net_price", col("l_extendedprice") * (1 - col("l_discount"))) \
            .withColumn("l_ship_year", year(col("l_shipdate"))) \
            .filter(col("l_quantity") > 0)


class OrdersTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("o_orderstatus", trim(col("o_orderstatus"))) \
            .withColumn("o_year", year(col("o_orderdate"))) \
            .withColumn("o_quarter", quarter(col("o_orderdate"))) \
            .filter(col("o_totalprice") > 0)


class PartTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("p_name", trim(col("p_name"))) \
            .withColumn("p_type", trim(col("p_type"))) \
            .withColumn("p_size_category",
                when(col("p_size") < 10, "SMALL")
                .when(col("p_size") < 30, "MEDIUM")
                .otherwise("LARGE")) \
            .filter(col("p_retailprice") > 0)


class SupplierTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("s_name", trim(col("s_name"))) \
            .withColumn("s_address", trim(col("s_address"))) \
            .withColumn("s_acctbal_status",
                when(col("s_acctbal") < 0, "NEGATIVE").otherwise("POSITIVE"))


class PartsuppTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.filter(col("ps_availqty") >= 0) \
            .filter(col("ps_supplycost") > 0)


class NationTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("n_name", trim(col("n_name")))


class RegionTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("r_name", trim(col("r_name")))