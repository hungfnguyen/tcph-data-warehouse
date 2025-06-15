select
    -- Primary key
    s_suppkey as supplier_key,
    
    -- Supplier attributes
    s_name as supplier_name,
    s_address as supplier_address,
    s_nationkey as nation_key,
    s_phone as supplier_phone,
    s_acctbal as account_balance,
    s_comment as supplier_comment,
    
    -- Add metadata
    current_timestamp() as dbt_updated_at

from parquet.`s3a://silver/supplier/`