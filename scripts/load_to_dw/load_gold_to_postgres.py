# scripts/load_to_dw/load_gold_to_postgres.py

import sys
from pyspark.sql import SparkSession

def load_gold_to_postgres(table_name):
    if not table_name:
        print("ERROR: Table name is required.")
        sys.exit(1)

    # --- 1. Tạo Spark Session với cấu hình kết nối MinIO (S3A) ---
    # Cấu hình này rất quan trọng để Spark có thể "nói chuyện" với MinIO.
    spark = SparkSession.builder \
        .appName(f"Load {table_name} from Gold to Postgres") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print(f"Spark Session created successfully for loading '{table_name}'.")

    try:
        # --- 2. Định nghĩa thông tin kết nối PostgreSQL ---
        # Các thông tin này phải khớp với cấu hình trong docker-compose.yml của bạn.
        # Lưu ý: host là 'postgres', là tên service trong Docker network.
        pg_url = "jdbc:postgresql://postgres:5432/tcph_dw"
        pg_properties = {
            "user": "postgres",
            "password": "postgres123",
            "driver": "org.postgresql.Driver"  # Tên class của driver JDBC
        }

        # --- 3. Đọc dữ liệu từ MinIO (Lớp Gold) ---
        # Đường dẫn này phải khớp với nơi dbt lưu các bảng của bạn.
        # input_path = f"s3a://gold/warehouse/{table_name}"
        input_path = f"file:///opt/spark/warehouse/gold.db/{table_name}"
        
        print(f"Reading data from Parquet source: {input_path}")
        df = spark.read.parquet(input_path)
        print(f"Successfully read {df.count()} rows for table '{table_name}'.")
        df.printSchema()

        # --- 4. Ghi dữ liệu vào PostgreSQL ---
        # Tên bảng đích sẽ nằm trong schema 'marts'
        target_table = f"marts.{table_name}"
        
        print(f"Writing data to PostgreSQL table: {target_table}")
        # Chế độ "overwrite" sẽ xóa dữ liệu cũ trong bảng và ghi lại dữ liệu mới.
        # Điều này giúp job có thể chạy lại nhiều lần mà không bị trùng lặp dữ liệu.
        df.write.jdbc(
            url=pg_url,
            table=target_table,
            mode="overwrite",
            properties=pg_properties
        )
        print(f"Successfully wrote data to {target_table}.")

    except Exception as e:
        print(f"An error occurred while processing table '{table_name}':")
        print(e)
    
    finally:
        # --- 5. Dừng Spark Session ---
        # Luôn luôn dừng session để giải phóng tài nguyên.
        print("Stopping Spark Session.")
        spark.stop()


if __name__ == "__main__":
    # Nhận tên bảng từ tham số dòng lệnh khi chạy spark-submit
    if len(sys.argv) != 2:
        print("Usage: spark-submit load_gold_to_postgres.py <table_name>")
        sys.exit(1)
    
    table_to_load = sys.argv[1]
    load_gold_to_postgres(table_to_load)