with mart as (
    select sum(delivered_order_count) as orders
    from {{ ref('mart_order_experience_daily') }}
), fact as (
    select count(*) as orders from {{ ref('fct_orders') }}
    where order_status = 'delivered'
)
select mart.orders, fact.orders
from mart cross join fact
where mart.orders <> fact.orders
