import sys
sys.path.append('.')

from extraction.snowflake_extractor import SnowflakeExtractor
from extraction.minio_client import MinIOClient
from extraction.config import TPC_H_TABLES

def main():
    print("Starting TPC-H extraction to Bronze layer")
    
    extractor = SnowflakeExtractor()
    minio_client = MinIOClient()
    
    try:
        for table_name in TPC_H_TABLES.keys():
            print(f"Processing {table_name}...")
            
            df_batches = extractor.extract_table_batches(table_name)
            paths = minio_client.save_table_batches(table_name, df_batches)
            
            print(f"Saved {table_name}: {len(paths)} batches")
        
        print("Extraction completed!")
        
    finally:
        extractor.close()

if __name__ == "__main__":
    main()