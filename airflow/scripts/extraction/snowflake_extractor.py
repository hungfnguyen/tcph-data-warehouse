import logging
import polars as pl
import snowflake.connector
from scripts.extraction.config import SNOWFLAKE_CONFIG, TPC_H_TABLES, INT64_COLS_MAP, DECIMAL_COLS_MAP
from scripts.extraction.minio_client import MinIOClient

logger = logging.getLogger(__name__)

class SnowflakeExtractor:
    def __init__(self):
        self.minio_client = MinIOClient()
    
    def _cast_for_spark_compatibility(self, df: pl.DataFrame, table_name: str) -> pl.DataFrame:
        """Ép kiểu dữ liệu để tương thích với Spark"""
        
        # Ép kiểu Int64
        cols_to_cast_int64 = INT64_COLS_MAP.get(table_name, [])
        for col_name in cols_to_cast_int64:
            if col_name in df.columns:
                df = df.with_columns(pl.col(col_name).cast(pl.Int64))
        
        # Ép kiểu Decimal
        cols_to_cast_decimal = DECIMAL_COLS_MAP.get(table_name, {})
        for col_name, (precision, scale) in cols_to_cast_decimal.items():
            if col_name in df.columns:
                df = df.with_columns(
                    pl.col(col_name)
                    .cast(pl.Float64)
                    .cast(pl.Decimal(precision, scale))
                )
        
        return df
    
    def extract_table_to_bronze(self, table_name: str, primary_key_col: str) -> str:
        """Extract table từ Snowflake vào MinIO Bronze layer"""
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        
        # Lấy chunk_size từ config
        chunk_size = TPC_H_TABLES.get(table_name, {}).get('chunk_size', 100000)
        logger.info(f"Extracting {table_name} with chunk size: {chunk_size}")

        # Đếm tổng số rows
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
        cursor.close()

        # Extract theo chunks
        chunks = []
        for offset in range(0, total_rows, chunk_size):
            query = f"""
                SELECT * FROM {table_name} 
                ORDER BY {primary_key_col} 
                LIMIT {chunk_size} OFFSET {offset}
            """
            chunk_df = pl.read_database(query, conn)
            chunks.append(chunk_df)
            logger.info(f"Extracted {len(chunk_df)} rows from {table_name} (offset: {offset})")

        # Concat tất cả chunks
        df = pl.concat(chunks)
        conn.close()

        # Ép kiểu cho Spark compatibility
        df = self._cast_for_spark_compatibility(df, table_name)
        
        # Save to MinIO Bronze
        object_path = f'{table_name.lower()}/{table_name.lower()}.parquet'
        full_path = self.minio_client.save_dataframe_to_parquet(df, 'bronze', object_path)
        
        logger.info(f"Successfully extracted {table_name}: {len(df)} rows")
        return full_path
    
    def get_table_info(self, table_name: str) -> dict:
        """Lấy thông tin về table (row count, columns, etc.)"""
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        
        # Get row count
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Get column info
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        columns = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            'table_name': table_name,
            'row_count': row_count,
            'columns': [{'name': col[0], 'type': col[1]} for col in columns]
        }