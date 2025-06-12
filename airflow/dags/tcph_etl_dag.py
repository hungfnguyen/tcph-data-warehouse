from __future__ import annotations

import logging
import polars as pl
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, year, quarter
from minio import Minio
import snowflake.connector
import io

logger = logging.getLogger(__name__)

# ===== Configuration =====


MINIO_CONFIG = {
    'endpoint': 'minio:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin123',
    'secure': False
}

TPC_H_TABLES = {
    'REGION': {'chunk_size': 100},
    'NATION': {'chunk_size': 100},
    'SUPPLIER': {'chunk_size': 25000},
    'CUSTOMER': {'chunk_size': 50000},
    'PART': {'chunk_size': 100000},
    'PARTSUPP': {'chunk_size': 200000},
    'ORDERS': {'chunk_size': 200000},
    'LINEITEM': {'chunk_size': 500000}
}

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False
}

@dag(
    dag_id="tcph_extract_transform_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    tags=["TCPH", "Snowflake", "Spark", "MinIO"],
    catchup=False
)
def tcph_etl_pipeline():
    
    # ============= EXTRACTION TASKS =============
    
    @task()
    def extract_region_to_bronze():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        df = pl.read_database("SELECT * FROM REGION", conn)
        conn.close()
        
        # Save to MinIO Bronze
        minio_client = Minio(**MINIO_CONFIG)
        if not minio_client.bucket_exists('bronze'):
            minio_client.make_bucket('bronze')
            
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        
        minio_client.put_object('bronze', 'region/region.parquet', buffer, buffer.getbuffer().nbytes)
        logger.info(f"Extracted REGION: {len(df)} rows to bronze")
        return f"bronze/region/region.parquet"

    @task()
    def extract_nation_to_bronze():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        df = pl.read_database("SELECT * FROM NATION", conn)
        conn.close()

        df = df.with_columns(pl.col("N_NATIONKEY").cast(pl.Int64))
        
        minio_client = Minio(**MINIO_CONFIG)
        if not minio_client.bucket_exists('bronze'):
            minio_client.make_bucket('bronze')
            
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        
        minio_client.put_object('bronze', 'nation/nation.parquet', buffer, buffer.getbuffer().nbytes)
        logger.info(f"Extracted NATION: {len(df)} rows to bronze")
        return f"bronze/nation/nation.parquet"

    # ============= TRANSFORMATION TASKS =============
    
    @task()
    def transform_region_bronze_to_silver():
        # Read from MinIO using MinIO client
        minio_client = Minio(**MINIO_CONFIG)
        
        # Download parquet file
        response = minio_client.get_object('bronze', 'region/region.parquet')
        data = response.read()
        buffer = io.BytesIO(data)
        
        # Read with Polars and transform
        df = pl.read_parquet(buffer)
        df_clean = df.with_columns(pl.col("R_NAME").str.strip_chars().alias("r_name_clean"))
        
        # Save back to MinIO Silver
        if not minio_client.bucket_exists('silver'):
            minio_client.make_bucket('silver')
            
        buffer_out = io.BytesIO()
        df_clean.write_parquet(buffer_out)
        buffer_out.seek(0)
        
        minio_client.put_object('silver', 'region/region.parquet', buffer_out, buffer_out.getbuffer().nbytes)
        logger.info(f"Transformed REGION: {len(df_clean)} rows to silver")

    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs={
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
            "spark.master": "spark://spark-master:7077",
            "spark.cores.max": "2",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        }
    )
    def transform_nation_bronze_to_silver(spark: SparkSession):
        df = spark.read.parquet("s3a://bronze/nation/")
        df_clean = df.withColumn("n_name", trim(col("n_name")))
        df_clean.coalesce(1).write.mode("overwrite").parquet("s3a://silver/nation/")
        logger.info(f"Transformed NATION: {df_clean.count()} rows to silver")

    # ============= TASK DEPENDENCIES =============
    
    # Extract tasks
    region_extract = extract_region_to_bronze()
    nation_extract = extract_nation_to_bronze() 
    
    # Transform tasks
    region_transform = transform_region_bronze_to_silver()
    nation_transform = transform_nation_bronze_to_silver()
    
    # Dependencies
    region_extract >> region_transform
    nation_extract >> nation_transform
tcph_etl_pipeline()