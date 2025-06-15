select
    -- Primary key
    n_nationkey as nation_key,
    
    -- Nation attributes
    n_name as nation_name,
    n_regionkey as region_key,
    n_comment as nation_comment,
    
    -- Add metadata
    current_timestamp() as dbt_updated_at

from parquet.`s3a://silver/nation/`