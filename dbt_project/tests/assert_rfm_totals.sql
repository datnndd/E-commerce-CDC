with mart as (
    select sum(frequency_orders) as orders, sum(monetary_brl) as revenue
    from {{ ref('mart_customer_rfm') }}
), fact as (
    select count(*) as orders, sum(item_subtotal_brl) as revenue
    from {{ ref('fct_orders') }}
    where order_status = 'delivered'
)
select mart.orders, fact.orders, mart.revenue, fact.revenue
from mart cross join fact
where mart.orders <> fact.orders or mart.revenue <> fact.revenue
