select
    -- Primary key
    r_regionkey as region_key,
    
    -- Region attributes
    r_name as region_name,
    r_comment as region_comment,
    
    -- Add metadata
    current_timestamp() as dbt_updated_at

from parquet.`s3a://silver/region/`