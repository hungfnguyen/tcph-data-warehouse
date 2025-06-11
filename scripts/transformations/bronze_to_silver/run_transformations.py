import sys
sys.path.append('.')

from scripts.transformations.utils.spark_session import create_spark_session, stop_spark_session
from scripts.transformations.bronze_to_silver.transformers import *

def main():
    spark = create_spark_session("Bronze-to-Silver")
    
    try:
        print("Starting Bronze to Silver transformations...")
        
        transformers = [
            CustomerTransformer(spark),
            LineitemTransformer(spark),
            OrdersTransformer(spark),
            PartTransformer(spark),
            SupplierTransformer(spark),
            PartsuppTransformer(spark),
            NationTransformer(spark),
            RegionTransformer(spark),
        ]
        
        for transformer in transformers:
            print(f"Processing {transformer.table_name}...")
            transformer.run()
        
        print("All transformations completed!")
        
    finally:
        stop_spark_session(spark)

if __name__ == "__main__":
    main()