import io
import logging
import polars as pl
from minio import Minio
from .config import MINIO_CONFIG

logger = logging.getLogger(__name__)

class MinIOClient:
    def __init__(self):
        self.client = Minio(**MINIO_CONFIG)
    
    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """Tạo bucket nếu chưa tồn tại"""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            logger.info(f"Created '{bucket_name}' bucket in MinIO.")
    
    def save_dataframe_to_parquet(self, 
                                  df: pl.DataFrame, 
                                  bucket: str, 
                                  object_path: str) -> str:
        """Lưu Polars DataFrame as Parquet vào MinIO"""
        self.ensure_bucket_exists(bucket)
        
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        
        self.client.put_object(bucket, object_path, buffer, buffer.getbuffer().nbytes)
        logger.info(f"Saved DataFrame: {len(df)} rows to {bucket}/{object_path}")
        return f"{bucket}/{object_path}"
    
    def read_parquet_to_dataframe(self, bucket: str, object_path: str) -> pl.DataFrame:
        """Đọc Parquet từ MinIO thành Polars DataFrame"""
        response = self.client.get_object(bucket, object_path)
        data = response.read()
        buffer = io.BytesIO(data)
        
        df = pl.read_parquet(buffer)
        logger.info(f"Read DataFrame: {len(df)} rows from {bucket}/{object_path}")
        return df
    
    def list_objects(self, bucket: str, prefix: str = "") -> list:
        """List objects in bucket với prefix"""
        objects = self.client.list_objects(bucket, prefix=prefix)
        return [obj.object_name for obj in objects]