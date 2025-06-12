from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add scripts to Python path
sys.path.append('/opt/airflow')

from scripts.extraction.snowflake_extractor import SnowflakeExtractor
from scripts.extraction.minio_client import MinIOClient
from scripts.transformations.utils.spark_session import create_spark_session
from scripts.transformations.bronze_to_silver.transformers import (
    CustomerTransformer, LineitemTransformer, OrdersTransformer,
    PartTransformer, SupplierTransformer, PartsuppTransformer,
    NationTransformer, RegionTransformer
)

# --- Constants ---
TABLES = ['CUSTOMER', 'LINEITEM', 'ORDERS', 'PART', 'SUPPLIER', 'PARTSUPP', 'NATION', 'REGION']

TRANSFORMER_MAP = {
    'CUSTOMER': CustomerTransformer,
    'LINEITEM': LineitemTransformer,
    'ORDERS': OrdersTransformer,
    'PART': PartTransformer,
    'SUPPLIER': SupplierTransformer,
    'PARTSUPP': PartsuppTransformer,
    'NATION': NationTransformer,
    'REGION': RegionTransformer,
}

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tcph_pipeline',
    default_args=default_args,
    description='TPC-H Data Pipeline: Snowflake -> Bronze -> Silver',
    schedule_interval='@daily',
    catchup=False,
    tags=['tpch', 'data-pipeline', 'etl'],
    max_active_runs=1,
)

# --- Task functions ---
def extract_table(table_name):
    print(f"[EXTRACT] Start extracting {table_name}")
    extractor = SnowflakeExtractor()
    minio_client = MinIOClient()
    try:
        df_batches = extractor.extract_table_batches(table_name)
        paths = minio_client.save_table_batches(table_name, df_batches)
        print(f"[EXTRACT] {table_name}: {len(paths)} batches saved to Bronze")
        return paths
    finally:
        extractor.close()

def transform_table(table_name):
    print(f"[TRANSFORM] Start transforming {table_name}")
    spark = create_spark_session(f"Transform-{table_name}")
    try:
        transformer_class = TRANSFORMER_MAP[table_name]
        transformer = transformer_class(spark)
        transformer.run()
        print(f"[TRANSFORM] {table_name} transformed to Silver")
    finally:
        spark.stop()

def cleanup_bronze():
    print("[CLEANUP] Old Bronze data cleaned (dummy step)")

# --- Task definitions ---
extract_tasks = []
transform_tasks = []

for table in TABLES:
    extract_task = PythonOperator(
        task_id=f'extract_{table.lower()}',
        python_callable=extract_table,
        op_kwargs={'table_name': table},
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f'transform_{table.lower()}',
        python_callable=transform_table,
        op_kwargs={'table_name': table},
        dag=dag,
    )

    extract_task >> transform_task
    extract_tasks.append(extract_task)
    transform_tasks.append(transform_task)

cleanup_task = PythonOperator(
    task_id='cleanup_bronze',
    python_callable=cleanup_bronze,
    dag=dag,
)

transform_tasks >> cleanup_task
