with mart as (
    select sum(item_count) as items, sum(delivered_item_revenue_brl) as revenue
    from {{ ref('mart_product_performance') }}
), fact as (
    select count(*) as items, sum(item_price_brl) as revenue
    from {{ ref('fct_order_items') }}
    where order_status = 'delivered'
)
select mart.items, fact.items, mart.revenue, fact.revenue
from mart cross join fact
where mart.items <> fact.items or mart.revenue <> fact.revenue
