with versions as (
    select customer_id as entity_id, 'customer' as entity_type,
           dbt_valid_from, dbt_valid_to,
           lead(dbt_valid_from) over (partition by customer_id order by dbt_valid_from) as next_valid_from
    from {{ ref('dim_customer') }}
    union all
    select product_id, 'product', dbt_valid_from, dbt_valid_to,
           lead(dbt_valid_from) over (partition by product_id order by dbt_valid_from)
    from {{ ref('dim_product') }}
)
select entity_id, entity_type
from versions
where (dbt_valid_to is not null and dbt_valid_to <= dbt_valid_from)
   or (next_valid_from is not null and (dbt_valid_to is null or dbt_valid_to > next_valid_from))
