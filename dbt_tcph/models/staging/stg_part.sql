select
    -- Primary key
    p_partkey as part_key,
    
    -- Part attributes
    p_name as part_name,
    p_mfgr as manufacturer,
    p_brand as brand,
    p_type as part_type,
    p_size as part_size,
    p_container as container,
    p_retailprice as retail_price,
    p_comment as part_comment,
    
    -- Add metadata
    current_timestamp() as dbt_updated_at

from parquet.`s3a://silver/part/`