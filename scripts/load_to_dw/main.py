import logging
from .ddl import create_postgres_engine, create_postgres_schemas_and_tables
from .loader import connect_spark_thrift, load_table_from_spark_to_postgres
from .config import SPARK_THRIFT_CONFIG, GOLD_TABLES_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Start loading Gold → PostgreSQL")

    pg_engine = create_postgres_engine()
    create_postgres_schemas_and_tables(pg_engine)

    spark_conn = connect_spark_thrift(SPARK_THRIFT_CONFIG)
    
    # Lấy danh sách các bảng từ config
    tables_to_load = GOLD_TABLES_CONFIG.keys()
    
    total_rows_all_tables = 0
    for table in tables_to_load:
        # Lấy chunk_size từ config cho bảng tương ứng
        # Sử dụng .get() để an toàn, nếu không có config sẽ lấy giá trị mặc định 50000
        chunk_size = GOLD_TABLES_CONFIG.get(table, {}).get('chunk_size', 50000)
        
        # Truyền chunk_size vào hàm load
        rows_loaded = load_table_from_spark_to_postgres(table, spark_conn, pg_engine, chunk_size)
        total_rows_all_tables += rows_loaded

    logger.info(f"All done. Loaded a total of {total_rows_all_tables} rows across all tables.")
    spark_conn.close()
    pg_engine.dispose()

if __name__ == "__main__":
    main()