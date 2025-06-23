import os
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv(override=True)

# Snowflake connection config
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'SNOWFLAKE_SAMPLE_DATA'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'TPCH_SF1'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'PUBLIC')
}

# MinIO configuration
MINIO_CONFIG = {
    'endpoint': 'minio:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin123',
    'secure': False
}

# TPC-H Tables configuration với chunk size
TPC_H_TABLES = {
    'REGION': {'chunk_size': 1000},
    'NATION': {'chunk_size': 1000},
    'SUPPLIER': {'chunk_size': 50000},
    'CUSTOMER': {'chunk_size': 100000},
    'PART': {'chunk_size': 100000},
    'PARTSUPP': {'chunk_size': 200000},
    'ORDERS': {'chunk_size': 300000},
    'LINEITEM': {'chunk_size': 500000}
}

# Airflow default args
DEFAULT_ARGS = {
    'owner': 'hungfnguyen',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['hungfnguyen.de@gmail.com']
}

# Spark configuration
DEFAULT_SPARK_CONFIG_KWARGS = {
    "spark.jars": "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.674.jar",
    
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

# Data type mappings for Spark compatibility
INT64_COLS_MAP = {
    'REGION': ['R_REGIONKEY'],
    'NATION': ['N_NATIONKEY', 'N_REGIONKEY'],
    'SUPPLIER': ['S_SUPPKEY', 'S_NATIONKEY'],
    'CUSTOMER': ['C_CUSTKEY', 'C_NATIONKEY'],
    'PART': ['P_PARTKEY'],
    'PARTSUPP': ['PS_PARTKEY', 'PS_SUPPKEY'],
    'ORDERS': ['O_ORDERKEY', 'O_CUSTKEY'],
    'LINEITEM': ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER']
}

DECIMAL_COLS_MAP = {
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

# Primary keys cho ordering
PRIMARY_KEYS = {
    'REGION': 'R_REGIONKEY',
    'NATION': 'N_NATIONKEY',
    'SUPPLIER': 'S_SUPPKEY',
    'CUSTOMER': 'C_CUSTKEY',
    'PART': 'P_PARTKEY',
    'PARTSUPP': 'PS_PARTKEY',
    'ORDERS': 'O_ORDERKEY',
    'LINEITEM': 'L_ORDERKEY'
}