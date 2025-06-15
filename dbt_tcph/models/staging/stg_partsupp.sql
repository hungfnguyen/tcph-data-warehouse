select
    -- Composite key
    ps_partkey as part_key,
    ps_suppkey as supplier_key,
    
    -- PartSupp attributes
    ps_availqty as available_qty,
    ps_supplycost as supply_cost,
    ps_comment as partsupp_comment,
    
    -- Add metadata
    current_timestamp() as dbt_updated_at

from parquet.`s3a://silver/partsupp/`