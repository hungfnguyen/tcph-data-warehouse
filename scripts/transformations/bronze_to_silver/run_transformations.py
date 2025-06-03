import sys
import logging
from datetime import datetime

# Add project root to path
sys.path.append('/home/hungfnguyen/data_engineer/projects/tcph-data-warehouse')

from scripts.transformations.utils.spark_session import create_spark_session, stop_spark_session
from scripts.transformations.bronze_to_silver.nation_transformer import NationTransformer
from scripts.transformations.bronze_to_silver.region_transformer import RegionTransformer
from scripts.transformations.bronze_to_silver.customer_transformer import CustomerTransformer
from scripts.transformations.bronze_to_silver.orders_transformer import OrdersTransformer
from scripts.transformations.bronze_to_silver.lineitem_transformer import LineitemTransformer
from scripts.transformations.bronze_to_silver.part_supplier_transformers import (
    PartTransformer, PartsuppTransformer, SupplierTransformer
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    start_time = datetime.now()
    logger.info("Starting Bronze to Silver transformations")
    
    # Create Spark session
    spark = create_spark_session("Bronze-to-Silver-Pipeline")
    
    # Define transformers in order
    transformers = [
        NationTransformer(spark),
        RegionTransformer(spark),
        SupplierTransformer(spark),
        CustomerTransformer(spark),
        PartTransformer(spark),
        PartsuppTransformer(spark),
        OrdersTransformer(spark),
        LineitemTransformer(spark)
    ]
    
    # Run each transformer
    for transformer in transformers:
        try:
            transformer.run()
        except Exception as e:
            logger.error(f"Failed to transform {transformer.table_name}: {str(e)}")
            raise
    
    # Stop Spark
    stop_spark_session(spark)
    
    # Report completion
    duration = datetime.now() - start_time
    logger.info(f"Completed all transformations in {duration}")


if __name__ == "__main__":
    main()