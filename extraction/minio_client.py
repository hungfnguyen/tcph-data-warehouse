import logging
import io
import polars as pl
from minio import Minio
from typing import Dict, List
from datetime import datetime

from extraction.config import MINIO_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MinIOClient:
    
    def __init__(self):
        self.client = Minio(
            endpoint=MINIO_CONFIG['endpoint'],
            access_key=MINIO_CONFIG['access_key'],
            secret_key=MINIO_CONFIG['secret_key'],
            secure=MINIO_CONFIG['secure']
        )
        self._create_buckets()
    
    def _create_buckets(self):
        buckets = ['bronze', 'silver', 'gold']
        for bucket in buckets:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
    
    def save_table_to_bronze(self, table_name: str, df: pl.DataFrame) -> str:
        # Convert to Parquet bytes
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"tpch/{table_name.lower()}/{table_name.lower()}_{timestamp}.parquet"
        
        # Upload to MinIO
        self.client.put_object(
            bucket_name='bronze',
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/parquet'
        )
        
        logger.info(f"Saved {table_name}: {len(df):,} rows to bronze/{object_name}")
        return f"bronze/{object_name}"
    
    def save_all_to_bronze(self, data: Dict[str, pl.DataFrame]) -> List[str]:
        saved_paths = []
        for table_name, df in data.items():
            path = self.save_table_to_bronze(table_name, df)
            saved_paths.append(path)
        
        logger.info(f"Saved {len(saved_paths)} tables to Bronze")
        return saved_paths
    
    def read_from_bronze(self, table_name: str, timestamp: str = None) -> pl.DataFrame:
        if timestamp:
            object_name = f"tpch/{table_name.lower()}/{table_name.lower()}_{timestamp}.parquet"
        else:
            # Get latest file for this table
            objects = self.client.list_objects('bronze', prefix=f"tpch/{table_name.lower()}/")
            latest_object = max(objects, key=lambda x: x.last_modified)
            object_name = latest_object.object_name
        
        # Download and convert to DataFrame
        response = self.client.get_object('bronze', object_name)
        data = response.read()
        buffer = io.BytesIO(data)
        df = pl.read_parquet(buffer)
        
        logger.info(f"Read {table_name}: {len(df):,} rows from bronze/{object_name}")
        return df
    
    def save_table_batches(self, table_name: str, df_batches):
        saved_paths = []
        batch_num = 1
        
        for df in df_batches:
            # Simple path without run_id
            object_name = f"{table_name.lower()}/batch_{batch_num:03d}.parquet"
            
            buffer = io.BytesIO()
            df.write_parquet(buffer)
            buffer.seek(0)
            
            self.client.put_object(
                bucket_name='bronze',
                object_name=object_name,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            
            saved_paths.append(f"bronze/{object_name}")
            logger.info(f"Saved {table_name} batch {batch_num}: {len(df):,} rows")
            batch_num += 1
        
        return saved_paths


# Convenience functions
def save_to_bronze(data: Dict[str, pl.DataFrame]) -> List[str]:
    client = MinIOClient()
    return client.save_all_to_bronze(data)

def read_table_from_bronze(table_name: str) -> pl.DataFrame:
    client = MinIOClient()
    return client.read_from_bronze(table_name)