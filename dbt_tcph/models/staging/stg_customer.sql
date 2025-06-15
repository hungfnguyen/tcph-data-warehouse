select
    -- Primary key
    c_custkey as customer_key,
    
    -- Customer attributes
    c_name as customer_name,
    c_address as customer_address,
    c_nationkey as nation_key,
    c_phone as customer_phone,
    c_acctbal as account_balance,
    c_mktsegment as market_segment,
    c_comment as customer_comment,
    
    -- Derived from Silver layer
    c_acctbal_status as account_balance_status,
    
    -- Add metadata
    current_timestamp() as dbt_updated_at

from parquet.`s3a://silver/customer/`