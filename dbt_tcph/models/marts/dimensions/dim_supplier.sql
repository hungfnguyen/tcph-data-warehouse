select 
    s.supplier_key,
    s.supplier_name,
    s.supplier_address,
    
    -- Nation and Region info (denormalized)
    s.nation_key,
    n.nation_name,
    n.region_key,
    r.region_name,
    
    -- Additional supplier info
    s.supplier_phone,
    s.account_balance,
    s.supplier_comment,
    
    current_timestamp() as dbt_updated_at

from {{ ref('stg_supplier') }} s
left join {{ ref('stg_nation') }} n 
    on s.nation_key = n.nation_key
left join {{ ref('stg_region') }} r 
    on n.region_key = r.region_key