with orders_by_region as (
    select o.*, coalesce(g.state, 'UNKNOWN') as customer_state
    from {{ ref('fct_orders') }} o
    left join {{ ref('dim_geography') }} g
      on o.customer_geography_key = g.geography_key
)
select o.purchase_date_key, d.date_day as purchase_date, o.customer_state,
       count(*) as order_count,
       sum(case when o.order_status = 'delivered' then 1 else 0 end) as delivered_order_count,
       sum(case when o.order_status = 'canceled' then 1 else 0 end) as canceled_order_count,
       sum(case when o.order_status = 'delivered' then o.item_subtotal_brl else 0 end) as delivered_item_revenue_brl,
       sum(case when o.order_status = 'delivered' then o.freight_total_brl else 0 end) as delivered_freight_brl,
       sum(case when o.order_status = 'delivered' then o.item_subtotal_brl + o.freight_total_brl else 0 end) as delivered_order_value_brl,
       cast(sum(case when o.order_status = 'delivered' then o.item_subtotal_brl else 0 end)
            / nullif(sum(case when o.order_status = 'delivered' then 1 else 0 end), 0) as decimal(18,2)) as aov_item_brl,
       cast(sum(case when o.order_status = 'canceled' then 1 else 0 end)
            / count(*) as decimal(10,4)) as cancellation_rate
from orders_by_region o
join {{ ref('dim_date') }} d on o.purchase_date_key = d.date_key
group by o.purchase_date_key, d.date_day, o.customer_state
