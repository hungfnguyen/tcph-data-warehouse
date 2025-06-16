SPARK_THRIFT_CONFIG = {
    'host': 'localhost',
    'port': 10000,
    'database': 'gold',
    'auth': 'NONE'
}

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tcph_dw',
    'user': 'postgres',
    'password': 'postgres123'
}

GOLD_TABLES_CONFIG = {
    # Bảng Dimension thường nhỏ hơn
    'dim_date': {'chunk_size': 50000},
    'dim_customer': {'chunk_size': 50000},
    'dim_supplier': {'chunk_size': 50000},
    'dim_part': {'chunk_size': 50000},

    # Bảng Fact rất lớn, cần chunk size hợp lý
    'fct_sales': {'chunk_size': 100000},
    'fct_inventory': {'chunk_size': 100000},
    'fct_revenue_by_supplier': {'chunk_size': 100000}
}