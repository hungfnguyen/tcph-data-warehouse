select
    -- Composite key
    l_orderkey as order_key,
    l_partkey as part_key,
    l_suppkey as supplier_key,
    l_linenumber as line_number,
    
    -- LineItem attributes
    l_quantity as quantity,
    l_extendedprice as extended_price,
    l_discount as discount,
    l_tax as tax,
    l_returnflag as return_flag,
    l_linestatus as line_status,
    l_shipdate as ship_date,
    l_commitdate as commit_date,
    l_receiptdate as receipt_date,
    l_shipinstruct as ship_instruct,
    l_shipmode as ship_mode,
    l_comment as lineitem_comment,
    
    -- Derived from Silver layer
    l_discount_percentage as discount_percentage,
    
    -- Add metadata
    current_timestamp() as dbt_updated_at

from parquet.`s3a://silver/lineitem/`