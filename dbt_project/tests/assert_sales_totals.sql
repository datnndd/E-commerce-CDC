with mart as (
    select sum(order_count) as orders,
           sum(delivered_item_revenue_brl) as revenue
    from {{ ref('mart_daily_sales') }}
), fact as (
    select count(*) as orders,
           sum(case when order_status = 'delivered' then item_subtotal_brl else 0 end) as revenue
    from {{ ref('fct_orders') }}
)
select mart.orders, fact.orders, mart.revenue, fact.revenue
from mart cross join fact
where mart.orders <> fact.orders or mart.revenue <> fact.revenue
