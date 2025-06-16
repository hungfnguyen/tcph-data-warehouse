import pandas as pd
from pyhive import hive
import logging
import traceback

logger = logging.getLogger(__name__)

def connect_spark_thrift(config):
    return hive.Connection(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        auth=config['auth']
    )

def load_table_from_spark_to_postgres(table_name: str, spark_conn, pg_engine, chunk_size: int):
    logger.info(f"Loading {table_name} from Spark with chunk size {chunk_size}...")
    total_rows = 0
    try:
        chunk_iterator = pd.read_sql(
            f"SELECT * FROM {table_name}",
            spark_conn,
            chunksize=chunk_size  # Tham số quyết định để tránh OOM
        )
        
        is_first_chunk = True
        for i, chunk_df in enumerate(chunk_iterator):
            num_rows = len(chunk_df)
            total_rows += num_rows
            logger.info(f"  - Processing chunk {i+1} for '{table_name}' with {num_rows} rows.")

            # if_exists='replace' cho chunk đầu tiên để tạo/thay thế bảng
            # if_exists='append' cho các chunk sau để nối tiếp
            write_mode = 'replace' if is_first_chunk else 'append'
            
            chunk_df.to_sql(
                name=table_name,
                con=pg_engine,
                schema='marts',
                if_exists=write_mode,
                index=False,
                method='multi',
                chunksize=10000 # chunksize ghi vào Postgres
            )
            
            is_first_chunk = False
            
        logger.info(f"Successfully saved {total_rows} rows for '{table_name}' to PostgreSQL.")
        return total_rows
        
    except Exception as e:
        logger.error(f"Failed to load {table_name} after processing {total_rows} rows.")
        logger.error(e)
        traceback.print_exc()
        return total_rows