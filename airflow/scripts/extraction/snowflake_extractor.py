import logging
import pandas as pd
import polars as pl
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from typing import Dict, Optional
from datetime import datetime

# from extraction.config import SNOWFLAKE_CONFIG, TPC_H_TABLES
from scripts.extraction.config import SNOWFLAKE_CONFIG, TPC_H_TABLES


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeExtractor:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or SNOWFLAKE_CONFIG
        self.conn = None
        self.engine = None
        self._connect()
    
    def _connect(self):
        logger.info("Connecting to Snowflake...")
        
        # Direct connection
        self.conn = snowflake.connector.connect(**self.config)
        
        # SQLAlchemy engine for pandas
        self.engine = create_engine(URL(**self.config))
        
        # Test connection
        cursor = self.conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        logger.info(f"Connected! Version: {version}")
    
    def get_table_info(self, table_name: str) -> Dict:
        cursor = self.conn.cursor()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Get columns
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        columns = [col[0] for col in cursor.fetchall()]
        
        return {
            'table_name': table_name,
            'row_count': row_count,
            'columns': columns
        }
    
    def extract_table(self, table_name: str) -> pl.DataFrame:
        logger.info(f"Extracting {table_name}...")
        
        table_config = TPC_H_TABLES[table_name]
        
        if table_config['size'] in ['tiny', 'small']:
            # Extract full table
            query = f"SELECT * FROM {table_name}"
            with self.engine.connect() as conn:
                pandas_df = pd.read_sql(query, conn)
        else:
            # Extract in chunks
            pandas_df = self._extract_chunks(table_name, table_config)
        
        # Convert to Polars
        polars_df = pl.from_pandas(pandas_df)
        
        # Add metadata
        # polars_df = polars_df.with_columns([
        #     pl.lit(datetime.now()).alias("_extracted_at"),
        #     pl.lit("snowflake").alias("_source"),
        #     pl.lit(table_name.lower()).alias("_table")
        # ])
        
        logger.info(f"Extracted {table_name}: {len(polars_df):,} rows")
        return polars_df
    
    def _extract_chunks(self, table_name: str, config: Dict) -> pd.DataFrame:
        chunk_size = config['chunk_size']
        primary_key = config['primary_key']
        
        if isinstance(primary_key, list):
            order_by = ', '.join(primary_key)
        else:
            order_by = primary_key
        
        # Get total rows
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
        
        logger.info(f"Extracting {total_rows:,} rows in chunks of {chunk_size:,}")
        
        # Extract chunks
        chunks = []
        for offset in range(0, total_rows, chunk_size):
            query = f"""
                SELECT * FROM {table_name} 
                ORDER BY {order_by}
                LIMIT {chunk_size} OFFSET {offset}
            """
            chunk = pd.read_sql(query, self.engine)
            chunks.append(chunk)
            
            logger.info(f"Chunk {len(chunks)}: {len(chunk):,} rows")
        
        return pd.concat(chunks, ignore_index=True)
    
    def extract_all_tables(self) -> Dict[str, pl.DataFrame]:
        logger.info("Extracting all TPC-H tables...")
        
        data = {}
        for table_name in TPC_H_TABLES.keys():
            try:
                data[table_name] = self.extract_table(table_name)
            except Exception as e:
                logger.error(f"Failed to extract {table_name}: {e}")
        
        total_rows = sum(len(df) for df in data.values())
        logger.info(f"Extracted {len(data)} tables, {total_rows:,} total rows")
        
        return data
    
    def extract_table_batches(self, table_name: str):
        """Extract table batch by batch, yield immediately"""
        logger.info(f"Extracting {table_name} in batches...")
        
        table_config = TPC_H_TABLES[table_name]
        
        if table_config['size'] in ['tiny', 'small']:
            # Small table, extract all
            query = f"SELECT * FROM {table_name}"
            cursor = self.conn.cursor()
            cursor.execute(query)
            
            # Fetch all and convert to pandas
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            pandas_df = pd.DataFrame(rows, columns=columns)
            polars_df = pl.from_pandas(pandas_df)
            
            logger.info(f"Batch 1: {len(polars_df):,} rows")
            yield polars_df
        else:
            # Large table, extract in batches
            chunk_size = table_config['chunk_size']
            primary_key = table_config['primary_key']
            
            if isinstance(primary_key, list):
                order_by = ', '.join(primary_key)
            else:
                order_by = primary_key
            
            # Get total rows
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]
            
            logger.info(f"Extracting {total_rows:,} rows in batches of {chunk_size:,}")
            
            batch_num = 1
            for offset in range(0, total_rows, chunk_size):
                query = f"""
                    SELECT * FROM {table_name} 
                    ORDER BY {order_by}
                    LIMIT {chunk_size} OFFSET {offset}
                """
                cursor = self.conn.cursor()
                cursor.execute(query)
                
                # Fetch batch and convert to pandas
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                pandas_df = pd.DataFrame(rows, columns=columns)
                polars_df = pl.from_pandas(pandas_df)
                
                logger.info(f"Batch {batch_num}: {len(polars_df):,} rows")
                yield polars_df
                batch_num += 1

    def close(self):
        if self.conn:
            self.conn.close()
        if self.engine:
            self.engine.dispose()
        logger.info("Connections closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Convenience functions
def extract_single_table(table_name: str) -> pl.DataFrame:
    with SnowflakeExtractor() as extractor:
        return extractor.extract_table(table_name)

def extract_all_data() -> Dict[str, pl.DataFrame]:
    with SnowflakeExtractor() as extractor:
        return extractor.extract_all_tables()