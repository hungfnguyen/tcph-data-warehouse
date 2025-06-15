select 
    -- Composite Primary Key
    ps.part_key,
    ps.supplier_key,
    
    -- Measures
    ps.available_qty,
    ps.supply_cost,
    
    -- Calculated measures
    ps.available_qty * ps.supply_cost as total_inventory_value,
    
    -- Part context (denormalized for performance)
    p.name as part_name,
    p.mfgr as part_mfgr,
    p.brand as part_brand,
    p.type as part_type,
    p.retail_price,
    
    -- Supplier context (denormalized for performance)
    s.supplier_name,
    s.nation_name as supplier_nation,
    s.region_name as supplier_region,
    
    -- Derived metrics
    case 
        when ps.available_qty > 1000 then 'High Stock'
        when ps.available_qty > 100 then 'Medium Stock'
        else 'Low Stock'
    end as stock_level,
    
    case 
        when ps.supply_cost < p.retail_price * 0.5 then 'Low Cost'
        when ps.supply_cost < p.retail_price * 0.7 then 'Medium Cost'
        else 'High Cost'
    end as cost_category,
    
    -- Metadata
    current_timestamp() as dbt_updated_at

from {{ ref('stg_partsupp') }} ps
left join {{ ref('dim_part') }} p 
    on ps.part_key = p.part_key
left join {{ ref('dim_supplier') }} s 
    on ps.supplier_key = s.supplier_key