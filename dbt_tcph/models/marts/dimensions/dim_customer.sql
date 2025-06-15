select 
    c.customer_key,
    c.customer_name,
    c.customer_address,
    c.market_segment,
    
    -- Nation and Region info (denormalized)
    c.nation_key,
    n.nation_name,
    n.region_key,
    r.region_name,
    
    -- Additional customer info
    c.customer_phone,
    c.account_balance,
    c.account_balance_status,
    c.customer_comment,
    
    current_timestamp() as dbt_updated_at

from {{ ref('stg_customer') }} c
left join {{ ref('stg_nation') }} n 
    on c.nation_key = n.nation_key
left join {{ ref('stg_region') }} r 
    on n.region_key = r.region_key