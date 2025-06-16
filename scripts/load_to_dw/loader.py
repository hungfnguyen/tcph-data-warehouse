import pandas as pd
from pyhive import hive
import logging

logger = logging.getLogger(__name__)

def connect_spark_thrift(config):
    return hive.Connection(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        auth=config['auth']
    )

def load_table_from_spark_to_postgres(table_name, spark_conn, pg_engine):
    try:
        logger.info(f"Loading {table_name} from Spark → PostgreSQL...")
        df = pd.read_sql(f"SELECT * FROM {table_name}", spark_conn)
        logger.info(f"Loaded {len(df)} rows from Spark")

        df.to_sql(
            name=table_name,
            con=pg_engine,
            schema='marts',
            if_exists='replace',
            index=False,
            chunksize=5000,
            method='multi'
        )
        logger.info(f"{table_name} saved to PostgreSQL")
        return len(df)
    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        raise
