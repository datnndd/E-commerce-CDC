with names as (
    select product_category_name from {{ ref('products_snapshot') }}
    where product_category_name is not null
    union
    select product_category_name from {{ ref('stg_category_translation') }}
    where product_category_name is not null
    union
    select '__unknown__' as product_category_name
)
select coalesce(n.product_category_name, '__unknown__') as category_key,
       n.product_category_name,
       coalesce(t.product_category_name_english, n.product_category_name, 'Unknown') as category_name_english
from names n
left join {{ ref('stg_category_translation') }} t
  on n.product_category_name = t.product_category_name
