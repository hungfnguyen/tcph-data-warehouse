import logging
import polars as pl
from datetime import datetime
from airflow.decorators import dag, task
from pyspark.sql import SparkSession

# Import custom modules
import sys
sys.path.append('/opt/airflow')

from scripts.extraction.config import (
    DEFAULT_ARGS, 
    DEFAULT_SPARK_CONFIG_KWARGS, 
    TPC_H_TABLES, 
    PRIMARY_KEYS
)
from scripts.extraction.snowflake_extractor import SnowflakeExtractor
from scripts.extraction.minio_client import MinIOClient
from scripts.transformation.spark_transforms import SparkTransformer

logger = logging.getLogger(__name__)

@dag(
    dag_id="tcph_extract_transform_pipeline_refactored",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    tags=["TCPH", "Snowflake", "Spark", "MinIO", "ETL", "Refactored"],
    catchup=False
)
def tcph_etl_pipeline_refactored():
    
    # Initialize extractors
    snowflake_extractor = SnowflakeExtractor()
    minio_client = MinIOClient()
    
    # ============= EXTRACTION TASKS =============
    
    @task(task_id="extract_region_to_bronze")
    def extract_region():
        return snowflake_extractor.extract_table_to_bronze('REGION', PRIMARY_KEYS['REGION'])
    
    @task(task_id="extract_nation_to_bronze")
    def extract_nation():
        return snowflake_extractor.extract_table_to_bronze('NATION', PRIMARY_KEYS['NATION'])
    
    @task(task_id="extract_supplier_to_bronze")
    def extract_supplier():
        return snowflake_extractor.extract_table_to_bronze('SUPPLIER', PRIMARY_KEYS['SUPPLIER'])
    
    @task(task_id="extract_customer_to_bronze")
    def extract_customer():
        return snowflake_extractor.extract_table_to_bronze('CUSTOMER', PRIMARY_KEYS['CUSTOMER'])
    
    @task(task_id="extract_part_to_bronze")
    def extract_part():
        return snowflake_extractor.extract_table_to_bronze('PART', PRIMARY_KEYS['PART'])
    
    @task(task_id="extract_partsupp_to_bronze")
    def extract_partsupp():
        return snowflake_extractor.extract_table_to_bronze('PARTSUPP', PRIMARY_KEYS['PARTSUPP'])
    
    @task(task_id="extract_orders_to_bronze")
    def extract_orders():
        return snowflake_extractor.extract_table_to_bronze('ORDERS', PRIMARY_KEYS['ORDERS'])
    
    @task(task_id="extract_lineitem_to_bronze")
    def extract_lineitem():
        return snowflake_extractor.extract_table_to_bronze('LINEITEM', PRIMARY_KEYS['LINEITEM'])
    
    # ============= TRANSFORMATION TASKS =============
    
    @task()
    def transform_region_bronze_to_silver():
        """Region transform using Polars (small table)"""
        df = minio_client.read_parquet_to_dataframe('bronze', 'region/region.parquet')
        df_clean = df.with_columns(pl.col("R_NAME").str.strip_chars().alias("r_name_clean"))
        
        object_path = 'region/region.parquet'
        minio_client.save_dataframe_to_parquet(df_clean, 'silver', object_path)
        logger.info(f"Transformed REGION: {len(df_clean)} rows to silver")
    
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_nation_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_nation(spark)
    
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_supplier_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_supplier(spark)
    
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_customer_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_customer(spark)
    
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_part_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_part(spark)
    
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_partsupp_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_partsupp(spark)
    
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_orders_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_orders(spark)
    
    @task.pyspark(
        conn_id="spark_connection",
        config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS
    )
    def transform_lineitem_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_lineitem(spark)
    
    # ============= TASK DEPENDENCIES =============
    
    # Extract tasks
    extract_tasks = [
        extract_region(),
        extract_nation(),
        extract_supplier(),
        extract_customer(),
        extract_part(),
        extract_partsupp(),
        extract_orders(),
        extract_lineitem()
    ]
    
    # Transform tasks
    transform_tasks = [
        transform_region_bronze_to_silver(),
        transform_nation_bronze_to_silver(),
        transform_supplier_bronze_to_silver(),
        transform_customer_bronze_to_silver(),
        transform_part_bronze_to_silver(),
        transform_partsupp_bronze_to_silver(),
        transform_orders_bronze_to_silver(),
        transform_lineitem_bronze_to_silver()
    ]
    
    # Set dependencies: Each extract -> corresponding transform
    extract_tasks[0] >> transform_tasks[0]  # region
    extract_tasks[1] >> transform_tasks[1]  # nation
    extract_tasks[2] >> transform_tasks[2]  # supplier
    extract_tasks[3] >> transform_tasks[3]  # customer
    extract_tasks[4] >> transform_tasks[4]  # part
    extract_tasks[5] >> transform_tasks[5]  # partsupp
    extract_tasks[6] >> transform_tasks[6]  # orders
    extract_tasks[7] >> transform_tasks[7]  # lineitem

# Initialize DAG
tcph_etl_pipeline_refactored()