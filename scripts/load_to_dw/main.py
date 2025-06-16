import logging
from .ddl import create_postgres_engine, create_postgres_schemas_and_tables
from .loader import connect_spark_thrift, load_table_from_spark_to_postgres
from .config import SPARK_THRIFT_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Start loading Gold → PostgreSQL")

    pg_engine = create_postgres_engine()
    create_postgres_schemas_and_tables(pg_engine)

    spark_conn = connect_spark_thrift(SPARK_THRIFT_CONFIG)
    
    tables = [
        'dim_date', 'dim_customer', 'dim_supplier', 'dim_part',
        'fct_sales', 'fct_inventory', 'fct_revenue_by_supplier'
    ]
    total_rows = 0
    for table in tables:
        total_rows += load_table_from_spark_to_postgres(table, spark_conn, pg_engine)

    logger.info(f"All done. Loaded {total_rows} rows")
    spark_conn.close()
    pg_engine.dispose()

if __name__ == "__main__":
    main()
