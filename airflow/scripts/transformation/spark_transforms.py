import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, year, quarter

logger = logging.getLogger(__name__)

class SparkTransformer:
    
    @staticmethod
    def transform_nation(spark: SparkSession):
        """Transform Nation table Bronze -> Silver"""
        df = spark.read.parquet("s3a://bronze/nation/")
        df_clean = (df
                   .withColumn("N_NAME", trim(col("N_NAME")))
                   .withColumn("N_COMMENT", trim(col("N_COMMENT"))))
        
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/nation/")
        logger.info(f"Transformed NATION: {df_clean.count()} rows to silver")
    
    @staticmethod
    def transform_supplier(spark: SparkSession):
        """Transform Supplier table Bronze -> Silver"""
        df = spark.read.parquet("s3a://bronze/supplier/")
        df_clean = (df
                   .withColumn("S_NAME", trim(col("S_NAME")))
                   .withColumn("S_ADDRESS", trim(col("S_ADDRESS")))
                   .withColumn("S_PHONE", trim(col("S_PHONE")))
                   .withColumn("S_COMMENT", trim(col("S_COMMENT"))))
        
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/supplier/")
        logger.info(f"Transformed SUPPLIER: {df_clean.count()} rows to silver")
    
    @staticmethod
    def transform_customer(spark: SparkSession):
        """Transform Customer table Bronze -> Silver"""
        df = spark.read.parquet("s3a://bronze/customer/")
        df_clean = (df
                   .withColumn("C_NAME", trim(col("C_NAME")))
                   .withColumn("C_ADDRESS", trim(col("C_ADDRESS")))
                   .withColumn("C_PHONE", trim(col("C_PHONE")))
                   .withColumn("C_MKTSEGMENT", trim(col("C_MKTSEGMENT")))
                   .withColumn("C_COMMENT", trim(col("C_COMMENT")))
                   .withColumn("C_ACCTBAL_STATUS",
                              when(col("C_ACCTBAL") < 0, "NEGATIVE").otherwise("POSITIVE")))
        
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/customer/")
        logger.info(f"Transformed CUSTOMER: {df_clean.count()} rows to silver")
    
    @staticmethod
    def transform_part(spark: SparkSession):
        """Transform Part table Bronze -> Silver"""
        df = spark.read.parquet("s3a://bronze/part/")
        df_clean = (df
                   .withColumn("P_NAME", trim(col("P_NAME")))
                   .withColumn("P_MFGR", trim(col("P_MFGR")))
                   .withColumn("P_BRAND", trim(col("P_BRAND")))
                   .withColumn("P_TYPE", trim(col("P_TYPE")))
                   .withColumn("P_CONTAINER", trim(col("P_CONTAINER")))
                   .withColumn("P_COMMENT", trim(col("P_COMMENT"))))
        
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/part/")
        logger.info(f"Transformed PART: {df_clean.count()} rows to silver")
    
    @staticmethod
    def transform_partsupp(spark: SparkSession):
        """Transform PartSupp table Bronze -> Silver"""
        df = spark.read.parquet("s3a://bronze/partsupp/")
        df_clean = df.withColumn("PS_COMMENT", trim(col("PS_COMMENT")))
        
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/partsupp/")
        logger.info(f"Transformed PARTSUPP: {df_clean.count()} rows to silver")
    
    @staticmethod
    def transform_orders(spark: SparkSession):
        """Transform Orders table Bronze -> Silver"""
        df = spark.read.parquet("s3a://bronze/orders/")
        df_clean = (df
                   .withColumn("O_ORDERSTATUS", trim(col("O_ORDERSTATUS")))
                   .withColumn("O_ORDERPRIORITY", trim(col("O_ORDERPRIORITY")))
                   .withColumn("O_CLERK", trim(col("O_CLERK")))
                   .withColumn("O_COMMENT", trim(col("O_COMMENT")))
                   .withColumn("O_YEAR", year(col("O_ORDERDATE")))
                   .withColumn("O_QUARTER", quarter(col("O_ORDERDATE")))
                   .filter(col("O_TOTALPRICE") > 0))
        
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/orders/")
        logger.info(f"Transformed ORDERS: {df_clean.count()} rows to silver")
    
    @staticmethod
    def transform_lineitem(spark: SparkSession):
        """Transform LineItem table Bronze -> Silver"""
        df = spark.read.parquet("s3a://bronze/lineitem/")
        df_clean = (df
                   .withColumn("L_RETURNFLAG", trim(col("L_RETURNFLAG")))
                   .withColumn("L_LINESTATUS", trim(col("L_LINESTATUS")))
                   .withColumn("L_SHIPINSTRUCT", trim(col("L_SHIPINSTRUCT")))
                   .withColumn("L_SHIPMODE", trim(col("L_SHIPMODE")))
                   .withColumn("L_COMMENT", trim(col("L_COMMENT")))
                   .withColumn("L_DISCOUNT_PERCENTAGE", col("L_DISCOUNT") * 100))
        
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/lineitem/")
        logger.info(f"Transformed LINEITEM: {df_clean.count()} rows to silver")