select s.*, s.dbt_scd_id as product_key,
       coalesce(s.product_category_name, '__unknown__') as category_key,
       row_number() over (partition by s.product_id order by s.dbt_valid_from) as version_number
from {{ ref('products_snapshot') }} s
