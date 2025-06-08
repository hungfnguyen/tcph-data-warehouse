import sys
sys.path.append('.')

from extraction.snowflake_extractor import extract_all_data
from extraction.minio_client import save_to_bronze

def main():
    print("Starting TPC-H extraction to Bronze layer")
    
    # Extract all tables from Snowflake
    data = extract_all_data()
    
    # Save to MinIO Bronze
    paths = save_to_bronze(data)
    
    print(f"Saved {len(paths)} tables to Bronze layer")

if __name__ == "__main__":
    main()