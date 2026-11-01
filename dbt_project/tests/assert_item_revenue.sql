with source_total as (
    select sum(price) as revenue from {{ ref('stg_order_items') }}
), fact_total as (
    select sum(item_price_brl) as revenue from {{ ref('fct_order_items') }}
), order_total as (
    select sum(item_subtotal_brl) as revenue from {{ ref('fct_orders') }}
)
select source_total.revenue as source_revenue,
       fact_total.revenue as item_fact_revenue,
       order_total.revenue as order_fact_revenue
from source_total cross join fact_total cross join order_total
where source_total.revenue <> fact_total.revenue
   or source_total.revenue <> order_total.revenue
