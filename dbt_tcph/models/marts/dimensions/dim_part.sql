select 
    part_key,
    part_name as name,
    manufacturer as mfgr,
    brand,
    part_type as type,
    part_size as size,
    container,
    retail_price,
    part_comment,
    dbt_updated_at

from {{ ref('stg_part') }}