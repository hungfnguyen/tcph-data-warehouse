# scripts/load_to_dw/6_load_gold_to_postgres_parquet.py

import sys
from pyspark.sql import SparkSession

def load_gold_to_postgres(table_name):
    """
    Load Parquet table từ MinIO → PostgreSQL
    """
    if not table_name:
        print("ERROR: Table name is required.")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName(f"Load {table_name} from Gold to Postgres") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print(f"Spark Session created for loading '{table_name}' from Parquet.")

    try:
        pg_url = "jdbc:postgresql://postgres:5432/tcph_dw"
        pg_properties = {
            "user": "postgres",
            "password": "postgres123",
            "driver": "org.postgresql.Driver"
        }

        # ✅ Đọc Parquet từ MinIO
        path = f"s3a://gold/warehouse/gold.db/{table_name}"
        print(f"Reading Parquet from: {path}")
        
        df = spark.read.parquet(path)
        
        row_count = df.count()
        print(f"✅ Successfully read {row_count} rows from Parquet")
        df.printSchema()
        print("Sample data:")
        df.show(5, truncate=False)

        # ✅ Ghi vào PostgreSQL
        target_table = f"marts.{table_name}"
        print(f"Writing {row_count} rows to PostgreSQL table: {target_table}")
        
        df.write.jdbc(
            url=pg_url,
            table=target_table,
            mode="overwrite",
            properties=pg_properties
        )
        
        print(f"🎉 SUCCESS: Loaded {row_count} rows to {target_table}")
        return row_count

    except Exception as e:
        print(f"❌ ERROR processing table '{table_name}':")
        print(e)
        import traceback
        traceback.print_exc()
        return 0
    
    finally:
        print("Stopping Spark Session.")
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit 6_load_gold_to_postgres_parquet.py <table_name>")
        sys.exit(1)
    
    table_to_load = sys.argv[1]
    rows_loaded = load_gold_to_postgres(table_to_load)
    
    if rows_loaded > 0:
        print(f"\n🎉 PIPELINE SUCCESS: {table_to_load} with {rows_loaded} rows")
    else:
        print(f"\n❌ PIPELINE FAILED: {table_to_load}")
        sys.exit(1)