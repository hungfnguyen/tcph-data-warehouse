select
    -- Primary key
    o_orderkey as order_key,
    
    -- Order attributes
    o_custkey as customer_key,
    o_orderstatus as order_status,
    o_totalprice as total_price,
    o_orderdate as order_date,
    o_orderpriority as order_priority,
    o_clerk as clerk,
    o_shippriority as ship_priority,
    o_comment as order_comment,
    
    -- Derived from Silver layer
    o_year as order_year,
    o_quarter as order_quarter,
    
    -- Add metadata
    current_timestamp() as dbt_updated_at

from parquet.`s3a://silver/orders/`