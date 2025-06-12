import os
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

# TPC-H table configurations
TPC_H_TABLES = {
    'REGION': {'size': 'tiny', 'chunk_size': 100, 'primary_key': 'R_REGIONKEY'},
    'NATION': {'size': 'tiny', 'chunk_size': 100, 'primary_key': 'N_NATIONKEY'},
    'SUPPLIER': {'size': 'small', 'chunk_size': 25000, 'primary_key': 'S_SUPPKEY'},
    'CUSTOMER': {'size': 'medium', 'chunk_size': 50000, 'primary_key': 'C_CUSTKEY'},
    'PART': {'size': 'medium', 'chunk_size': 100000, 'primary_key': 'P_PARTKEY'},
    'PARTSUPP': {'size': 'large', 'chunk_size': 200000, 'primary_key': ['PS_PARTKEY', 'PS_SUPPKEY']},
    'ORDERS': {'size': 'large', 'chunk_size': 200000, 'primary_key': 'O_ORDERKEY'},
    'LINEITEM': {'size': 'huge', 'chunk_size': 500000, 'primary_key': ['L_ORDERKEY', 'L_LINENUMBER']}
}

# MinIO config
MINIO_CONFIG = {
    'endpoint': 'minio:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin123',
    'secure': False
}