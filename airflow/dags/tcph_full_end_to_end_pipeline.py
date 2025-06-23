import logging
from datetime import datetime
import polars as pl

from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from pyspark.sql import SparkSession

import sys
sys.path.append('/opt/airflow')

from scripts.extraction.config import (
    DEFAULT_ARGS,
    DEFAULT_SPARK_CONFIG_KWARGS,
    PRIMARY_KEYS
)
from scripts.extraction.snowflake_extractor import SnowflakeExtractor
from scripts.extraction.minio_client import MinIOClient
from scripts.transformation.spark_transforms import SparkTransformer

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt_tcph"
SPARK_JOBS_DIR = "/opt/airflow/scripts/load_to_dw"
JARS_DIR = "/opt/airflow/jars"

ALL_JARS = ",".join([
    f"{JARS_DIR}/postgresql-42.7.7.jar",
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.674.jar",
])

DIM_TABLES = ["dim_date", "dim_customer", "dim_supplier", "dim_part"]
FACT_TABLES = ["fct_inventory", "fct_sales", "fct_revenue_by_supplier"]



@dag(
    dag_id="tcph_full_end_to_end_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 6, 23),
    schedule_interval="30 0 * * *",
    tags=["TCPH", "ETL", "dbt", "Full Pipeline"],
    catchup=False
)
def full_tcph_pipeline():
    
    snowflake_extractor = SnowflakeExtractor()
    minio_client = MinIOClient()


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

    @task(task_id="transform_region_bronze_to_silver")
    def transform_region_bronze_to_silver():
        df = minio_client.read_parquet_to_dataframe('bronze', 'region/region.parquet')
        df_clean = df.with_columns(pl.col("R_NAME").str.strip_chars().alias("r_name_clean"))
        minio_client.save_dataframe_to_parquet(df_clean, 'silver', 'region/region.parquet')
    
    @task.pyspark(conn_id="spark_connection", config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS)
    def transform_nation_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_nation(spark)
    @task.pyspark(conn_id="spark_connection", config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS)
    def transform_supplier_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_supplier(spark)
    @task.pyspark(conn_id="spark_connection", config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS)
    def transform_customer_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_customer(spark)
    @task.pyspark(conn_id="spark_connection", config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS)
    def transform_part_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_part(spark)
    @task.pyspark(conn_id="spark_connection", config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS)
    def transform_partsupp_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_partsupp(spark)
    @task.pyspark(conn_id="spark_connection", config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS)
    def transform_orders_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_orders(spark)
    @task.pyspark(conn_id="spark_connection", config_kwargs=DEFAULT_SPARK_CONFIG_KWARGS)
    def transform_lineitem_bronze_to_silver(spark: SparkSession):
        SparkTransformer.transform_lineitem(spark)

    extract_tasks = [extract_region(), extract_nation(), extract_supplier(), extract_customer(), extract_part(), extract_partsupp(), extract_orders(), extract_lineitem()]
    transform_tasks = [transform_region_bronze_to_silver(), transform_nation_bronze_to_silver(), transform_supplier_bronze_to_silver(), transform_customer_bronze_to_silver(), transform_part_bronze_to_silver(), transform_partsupp_bronze_to_silver(), transform_orders_bronze_to_silver(), transform_lineitem_bronze_to_silver()]
    
    for i in range(len(extract_tasks)):
        extract_tasks[i] >> transform_tasks[i]

    
    dbt_run_task = BashOperator(
        task_id="run_dbt_gold_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
    )

    with TaskGroup(group_id="load_dims") as load_dims_group:
        for tbl in DIM_TABLES:
            SparkSubmitOperator(
                task_id=f"load_{tbl}",
                application=f"{SPARK_JOBS_DIR}/load_gold_to_postgres.py",
                application_args=[tbl],
                conn_id="spark_default",
                jars=ALL_JARS,
                driver_memory="2g",
                executor_memory="1g",
            )

    fact_tasks = []
    for tbl in FACT_TABLES:
        fact_task = SparkSubmitOperator(
            task_id=f"load_{tbl}",
            application=f"{SPARK_JOBS_DIR}/load_gold_to_postgres.py",
            application_args=[tbl],
            conn_id="spark_default",
            jars=ALL_JARS,
            driver_memory="2g",
            executor_memory="1g",
        )
        fact_tasks.append(fact_task)
    
    for i in range(len(fact_tasks) - 1):
        fact_tasks[i] >> fact_tasks[i+1]


    
    transform_tasks >> dbt_run_task >> load_dims_group >> fact_tasks[0]


full_tcph_pipeline()