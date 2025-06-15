with date_range as (
    -- Get date range from orders
    select 
        min(order_date) as min_date,
        max(order_date) as max_date
    from {{ ref('stg_orders') }}
),

date_spine as (
    -- Generate date sequence
    select 
        explode(sequence(
            (select min_date from date_range),
            (select max_date from date_range),
            interval 1 day
        )) as date_key
)

select 
    date_key,
    year(date_key) as year,
    month(date_key) as month,
    day(date_key) as day,
    quarter(date_key) as quarter,
    weekofyear(date_key) as week_of_year,
    dayofweek(date_key) as day_of_week,
    case when dayofweek(date_key) in (1, 7) then true else false end as is_weekend,
    current_timestamp() as dbt_updated_at

from date_spine