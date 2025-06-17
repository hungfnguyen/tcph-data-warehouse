# dags/dbt_spark_pipeline_dag.py

from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_tcph"
SPARK_JOBS_DIR = "/opt/airflow/spark_jobs"
JARS_DIR = "/opt/airflow/jars"

ALL_JARS = ",".join([
    f"{JARS_DIR}/postgresql-42.7.7.jar",
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.674.jar",
])

DIM_TABLES  = ["dim_date", "dim_customer", "dim_supplier", "dim_part"]
FACT_TABLES = ["fct_inventory", "fct_sales", "fct_revenue_by_supplier"]

with DAG(
    dag_id="tcph_dbt_spark_pipeline_v2",
    start_date=datetime(2025, 6, 17),
    schedule_interval=None,
    catchup=False,
    tags=["tcph", "dbt", "spark"],
) as dag:
    
    # Task chạy dbt
    dbt_run_task = BashOperator(
        task_id="run_dbt_gold_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
    )

    from airflow.utils.task_group import TaskGroup
    with TaskGroup(group_id="load_dims") as load_dims:
        for tbl in DIM_TABLES:
            SparkSubmitOperator(
                task_id=f"load_{tbl}",
                application=f"{SPARK_JOBS_DIR}/load_gold_to_postgres_parquet.py",
                application_args=[tbl],
                conn_id="spark_default",
                jars=ALL_JARS,
                driver_memory="2g",
                executor_memory="1g",
            )

    first_fact_task = None
    prev_task       = None

    for tbl in FACT_TABLES:
        fact_task = SparkSubmitOperator(
            task_id=f"load_{tbl}",
            application=f"{SPARK_JOBS_DIR}/load_gold_to_postgres_parquet.py",
            application_args=[tbl],
            conn_id="spark_default",
            jars=ALL_JARS,
            driver_memory="2g",
            executor_memory="1g",
        )

        if prev_task:
            prev_task >> fact_task
        else:
            first_fact_task = fact_task

        prev_task = fact_task

    # -------- dependency tổng thể ----------
    dbt_run_task >> load_dims >> first_fact_task