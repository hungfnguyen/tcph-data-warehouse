select 
    -- Composite Primary Key
    l.order_key,
    l.line_number,
    
    -- Foreign Keys
    o.customer_key,
    l.part_key,
    l.supplier_key,
    o.order_date,
    l.ship_date,
    
    -- Measures
    l.quantity,
    l.extended_price,
    l.discount,
    l.tax,
    
    -- Calculated measures
    l.extended_price * (1 - l.discount) as net_price,
    l.extended_price * (1 - l.discount) * (1 + l.tax) as total_price,
    l.extended_price * l.discount as discount_amount,
    
    -- Order context
    o.order_status,
    o.order_priority,
    o.total_price as order_total_price,
    
    -- Lineitem context
    l.return_flag,
    l.line_status,
    l.ship_mode,
    l.commit_date,
    l.receipt_date,
    
    -- Metadata
    current_timestamp() as dbt_updated_at

from {{ ref('stg_lineitem') }} l
inner join {{ ref('stg_orders') }} o 
    on l.order_key = o.order_key