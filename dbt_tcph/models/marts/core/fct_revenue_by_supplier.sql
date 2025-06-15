with daily_supplier_sales as (
    select 
        supplier_key,
        order_date as date_key,
        count(distinct order_key) as total_orders,
        count(*) as total_line_items,
        sum(quantity) as total_quantity,
        sum(extended_price) as gross_revenue,
        sum(net_price) as net_revenue,
        sum(total_price) as total_revenue,
        sum(discount_amount) as total_discount,
        avg(extended_price) as avg_line_value,
        max(extended_price) as max_line_value,
        min(extended_price) as min_line_value
    from {{ ref('fct_sales') }}
    group by supplier_key, order_date
)

select 
    -- Composite Primary Key
    supplier_key,
    date_key,
    
    -- Core Measures
    total_orders,
    total_line_items,
    total_quantity,
    net_revenue as revenue,
    
    -- Additional Measures
    gross_revenue,
    total_revenue,
    total_discount,
    avg_line_value,
    max_line_value,
    min_line_value,
    
    -- Calculated KPIs
    net_revenue / total_orders as revenue_per_order,
    net_revenue / total_line_items as revenue_per_line,
    total_discount / gross_revenue as discount_rate,
    
    -- Performance categories
    case 
        when net_revenue > 50000 then 'High Performer'
        when net_revenue > 10000 then 'Medium Performer'
        else 'Low Performer'
    end as performance_tier,
    
    -- Metadata
    current_timestamp() as dbt_updated_at

from daily_supplier_sales