from __future__ import annotations

import logging
import polars as pl
import pandas as pd # Although not used directly in new tasks, kept for consistency
from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, year, quarter, lit
from minio import Minio
import snowflake.connector
import io
import sys

sys.path.append('/opt/airflow')
from scripts.extraction.config import SNOWFLAKE_CONFIG, MINIO_CONFIG, TPC_H_TABLES

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False
}

# Cấu hình Spark mặc định cho các PySpark task
# Bao gồm các JAR cần thiết để kết nối với MinIO (S3A)
DEFAULT_SPARK_CONFIG_KWARGS = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
    "spark.master": "spark://spark-master:7077",
    "spark.cores.max": "4",
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
}

@dag(
    dag_id="tcph_extract_transform_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    tags=["TCPH", "Snowflake", "Spark", "MinIO", "ETL"],
    catchup=False
)
def tcph_etl_pipeline():
    # Hàm trợ giúp để ép kiểu dữ liệu cho Spark compatibility
    def _cast_for_spark_compatibility(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
        # Cột cần ép kiểu Int64 (tương ứng với BIGINT trong Spark)
        int64_cols_map = {
            'REGION': ['R_REGIONKEY'],
            'NATION': ['N_NATIONKEY', 'N_REGIONKEY'],
            'SUPPLIER': ['S_SUPPKEY', 'S_NATIONKEY'],
            'CUSTOMER': ['C_CUSTKEY', 'C_NATIONKEY'],
            'PART': ['P_PARTKEY'],
            'PARTSUPP': ['PS_PARTKEY', 'PS_SUPPKEY'],
            'ORDERS': ['O_ORDERKEY', 'O_CUSTKEY'],
            'LINEITEM': ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER']
        }
        
        # Cột cần ép kiểu Decimal(precision, scale)
        # Theo chuẩn TPC-H, hầu hết các giá trị tiền tệ/số lượng là DECIMAL(15,2)
        decimal_cols_map = {
            'SUPPLIER': {'S_ACCTBAL': (15, 2)},
            'CUSTOMER': {'C_ACCTBAL': (15, 2)},
            'PART': {'P_RETAILPRICE': (15, 2)},
            'PARTSUPP': {'PS_AVAILQTY': (15,2), 'PS_SUPPLYCOST': (15, 2)},
            'ORDERS': {'O_TOTALPRICE': (15, 2)},
            'LINEITEM': {
                'L_QUANTITY': (15, 2),
                'L_EXTENDEDPRICE': (15, 2),
                'L_DISCOUNT': (15, 2),
                'L_TAX': (15, 2)
            }
        }

        # Ép kiểu Int64
        cols_to_cast_int64 = int64_cols_map.get(table_name, [])
        for col_name in cols_to_cast_int64:
            if col_name in df.columns:
                df = df.with_columns(pl.col(col_name).cast(pl.Int64))
        
        # Ép kiểu Decimal
        cols_to_cast_decimal = decimal_cols_map.get(table_name, {})
        for col_name, (precision, scale) in cols_to_cast_decimal.items():
            if col_name in df.columns:
                # Ép kiểu sang Float64 trước để xử lý tốt các kiểu dữ liệu nguồn khác nhau
                # rồi sau đó ép sang Decimal với precision và scale cụ thể
                df = df.with_columns(pl.col(col_name).cast(pl.Float64).cast(pl.Decimal(precision, scale)))
        
        return df

    # Hàm chung cho các task trích xuất Polars
    def _extract_table_to_bronze(table_name: str, primary_key_col: str):
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        
        # Lấy chunk_size từ cấu hình TPC_H_TABLES
        chunk_size = TPC_H_TABLES.get(table_name, {}).get('chunk_size', 100000) # Mặc định 100k nếu không tìm thấy

        logger.info(f"Extracting {table_name} with chunk size: {chunk_size}")

        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
        cursor.close()

        chunks = []
        for offset in range(0, total_rows, chunk_size):
            # ORDER BY primary_key_col để đảm bảo thứ tự khi phân mảnh
            query = f"SELECT * FROM {table_name} ORDER BY {primary_key_col} LIMIT {chunk_size} OFFSET {offset}"
            chunk_df = pl.read_database(query, conn)
            chunks.append(chunk_df)
            logger.info(f"Extracted {len(chunk_df)} rows from {table_name} (offset: {offset})")

        df = pl.concat(chunks)
        conn.close()

        # Ép kiểu các cột KEY và DECIMAL để tương thích với Spark
        df = _cast_for_spark_compatibility(df, table_name)
        
        # Save to MinIO Bronze
        minio_client = Minio(**MINIO_CONFIG)
        if not minio_client.bucket_exists('bronze'):
            minio_client.make_bucket('bronze')
            logger.info("Created 'bronze' bucket in MinIO.")
            
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        
        object_path = f'{table_name.lower()}/{table_name.lower()}.parquet'
        minio_client.put_object('bronze', object_path, buffer, buffer.getbuffer().nbytes)
        logger.info(f"Extracted {table_name}: {len(df)} rows to bronze/{object_path}")
        return f"bronze/{object_path}"


    # ============= EXTRACTION TASKS (Polars) =============

    # Region
    extract_region_to_bronze = task(
        _extract_table_to_bronze, 
        task_id="extract_region_to_bronze"
    )(table_name='REGION', primary_key_col='R_REGIONKEY')

    # Nation
    extract_nation_to_bronze = task(
        _extract_table_to_bronze,
        task_id="extract_nation_to_bronze"
    )(table_name='NATION', primary_key_col='N_NATIONKEY')

    # Supplier
    extract_supplier_to_bronze = task(
        _extract_table_to_bronze,
        task_id="extract_supplier_to_bronze"
    )(table_name='SUPPLIER', primary_key_col='S_SUPPKEY')

    # Customer
    extract_customer_to_bronze = task(
        _extract_table_to_bronze,
        task_id="extract_customer_to_bronze"
    )(table_name='CUSTOMER', primary_key_col='C_CUSTKEY')

    # Part
    extract_part_to_bronze = task(
        _extract_table_to_bronze,
        task_id="extract_part_to_bronze"
    )(table_name='PART', primary_key_col='P_PARTKEY')

    # PartSupp (Composite key, use a unique part of it for ORDER BY)
    extract_partsupp_to_bronze = task(
        _extract_table_to_bronze,
        task_id="extract_partsupp_to_bronze"
    )(table_name='PARTSUPP', primary_key_col='PS_PARTKEY') # Or PS_SUPPKEY, just need a consistent order

    # Orders
    extract_orders_to_bronze = task(
        _extract_table_to_bronze,
        task_id="extract_orders_to_bronze"
    )(table_name='ORDERS', primary_key_col='O_ORDERKEY')

    # LineItem (Composite key, use a unique part of it for ORDER BY)
    extract_lineitem_to_bronze = task(
        _extract_table_to_bronze,
        task_id="extract_lineitem_to_bronze"
    )(table_name='LINEITEM', primary_key_col='L_ORDERKEY') # Or L_LINENUMBER, just need a consistent order


    # ============= TRANSFORMATION TASKS (Spark) =============

    # Region (Polars transform, for consistency and small data)
    @task()
    def transform_region_bronze_to_silver():
        minio_client = Minio(**MINIO_CONFIG)
        response = minio_client.get_object('bronze', 'region/region.parquet')
        data = response.read()
        buffer = io.BytesIO(data)
        
        df = pl.read_parquet(buffer)
        df_clean = df.with_columns(pl.col("R_NAME").str.strip_chars().alias("r_name_clean"))
        
        if not minio_client.bucket_exists('silver'):
            minio_client.make_bucket('silver')
            logger.info("Created 'silver' bucket in MinIO.")
            
        buffer_out = io.BytesIO()
        df_clean.write_parquet(buffer_out)
        buffer_out.seek(0)
        
        minio_client.put_object('silver', 'region/region.parquet', buffer_out, buffer_out.getbuffer().nbytes)
        logger.info(f"Transformed REGION: {len(df_clean)} rows to silver")


    # Nation (Spark transform)
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_nation_bronze_to_silver(spark: SparkSession):
        df = spark.read.parquet("s3a://bronze/nation/")
        # Convert all string columns to lowercase and trim spaces
        df_clean = df.withColumn("N_NAME", trim(col("N_NAME"))) \
                     .withColumn("N_COMMENT", trim(col("N_COMMENT")))
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/nation/")
        logger.info(f"Transformed NATION: {df_clean.count()} rows to silver")

    # Supplier (Spark transform)
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_supplier_bronze_to_silver(spark: SparkSession):
        df = spark.read.parquet("s3a://bronze/supplier/")
        df_clean = df.withColumn("S_NAME", trim(col("S_NAME"))) \
                     .withColumn("S_ADDRESS", trim(col("S_ADDRESS"))) \
                     .withColumn("S_PHONE", trim(col("S_PHONE"))) \
                     .withColumn("S_COMMENT", trim(col("S_COMMENT")))
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/supplier/")
        logger.info(f"Transformed SUPPLIER: {df_clean.count()} rows to silver")

    # Customer (Spark transform)
    @task.pyspark(
        conn_id="spark_connection", 
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_customer_bronze_to_silver(spark: SparkSession):
        df = spark.read.parquet("s3a://bronze/customer/")
        df_clean = df.withColumn("C_NAME", trim(col("C_NAME"))) \
                     .withColumn("C_ADDRESS", trim(col("C_ADDRESS"))) \
                     .withColumn("C_PHONE", trim(col("C_PHONE"))) \
                     .withColumn("C_MKTSEGMENT", trim(col("C_MKTSEGMENT"))) \
                     .withColumn("C_COMMENT", trim(col("C_COMMENT"))) \
                     .withColumn("C_ACCTBAL_STATUS",
                                 when(col("C_ACCTBAL") < 0, "NEGATIVE").otherwise("POSITIVE"))
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/customer/")
        logger.info(f"Transformed CUSTOMER: {df_clean.count()} rows to silver")

    # Part (Spark transform)
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_part_bronze_to_silver(spark: SparkSession):
        df = spark.read.parquet("s3a://bronze/part/")
        df_clean = df.withColumn("P_NAME", trim(col("P_NAME"))) \
                     .withColumn("P_MFGR", trim(col("P_MFGR"))) \
                     .withColumn("P_BRAND", trim(col("P_BRAND"))) \
                     .withColumn("P_TYPE", trim(col("P_TYPE"))) \
                     .withColumn("P_CONTAINER", trim(col("P_CONTAINER"))) \
                     .withColumn("P_COMMENT", trim(col("P_COMMENT")))
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/part/")
        logger.info(f"Transformed PART: {df_clean.count()} rows to silver")

    # PartSupp (Spark transform)
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_partsupp_bronze_to_silver(spark: SparkSession):
        df = spark.read.parquet("s3a://bronze/partsupp/")
        df_clean = df.withColumn("PS_COMMENT", trim(col("PS_COMMENT")))
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/partsupp/")
        logger.info(f"Transformed PARTSUPP: {df_clean.count()} rows to silver")

    # Orders (Spark transform)
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_orders_bronze_to_silver(spark: SparkSession):
        df = spark.read.parquet("s3a://bronze/orders/")
        df_clean = df.withColumn("O_ORDERSTATUS", trim(col("O_ORDERSTATUS"))) \
                     .withColumn("O_ORDERPRIORITY", trim(col("O_ORDERPRIORITY"))) \
                     .withColumn("O_CLERK", trim(col("O_CLERK"))) \
                     .withColumn("O_COMMENT", trim(col("O_COMMENT"))) \
                     .withColumn("O_YEAR", year(col("O_ORDERDATE"))) \
                     .withColumn("O_QUARTER", quarter(col("O_ORDERDATE"))) \
                     .filter(col("O_TOTALPRICE") > 0) # Filtering example
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/orders/")
        logger.info(f"Transformed ORDERS: {df_clean.count()} rows to silver")

    # LineItem (Spark transform)
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_lineitem_bronze_to_silver(spark: SparkSession):
        df = spark.read.parquet("s3a://bronze/lineitem/")
        # Basic trimming for string columns
        df_clean = df.withColumn("L_RETURNFLAG", trim(col("L_RETURNFLAG"))) \
                     .withColumn("L_LINESTATUS", trim(col("L_LINESTATUS"))) \
                     .withColumn("L_SHIPINSTRUCT", trim(col("L_SHIPINSTRUCT"))) \
                     .withColumn("L_SHIPMODE", trim(col("L_SHIPMODE"))) \
                     .withColumn("L_COMMENT", trim(col("L_COMMENT"))) \
                     .withColumn("L_DISCOUNT_PERCENTAGE", col("L_DISCOUNT") * 100) # Example: derived column
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/lineitem/")
        logger.info(f"Transformed LINEITEM: {df_clean.count()} rows to silver")


    # ============= TASK DEPENDENCIES =============

    # Ánh xạ các tác vụ trích xuất
    extract_tasks = {
        'REGION': extract_region_to_bronze,
        'NATION': extract_nation_to_bronze,
        'SUPPLIER': extract_supplier_to_bronze,
        'CUSTOMER': extract_customer_to_bronze,
        'PART': extract_part_to_bronze,
        'PARTSUPP': extract_partsupp_to_bronze,
        'ORDERS': extract_orders_to_bronze,
        'LINEITEM': extract_lineitem_to_bronze
    }

    # Ánh xạ các tác vụ biến đổi
    transform_tasks = {
        'REGION': transform_region_bronze_to_silver(),
        'NATION': transform_nation_bronze_to_silver(),
        'SUPPLIER': transform_supplier_bronze_to_silver(),
        'CUSTOMER': transform_customer_bronze_to_silver(),
        'PART': transform_part_bronze_to_silver(),
        'PARTSUPP': transform_partsupp_bronze_to_silver(),
        'ORDERS': transform_orders_bronze_to_silver(),
        'LINEITEM': transform_lineitem_bronze_to_silver()
    }

    # Thiết lập dependencies: Extract -> Transform cho từng bảng
    for table_name in TPC_H_TABLES.keys():
        extract_task = extract_tasks[table_name]
        transform_task = transform_tasks[table_name]
        extract_task >> transform_task

# Khởi tạo DAG
tcph_etl_pipeline()