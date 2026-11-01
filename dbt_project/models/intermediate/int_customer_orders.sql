select c.customer_unique_id,
       count(*) as delivered_order_count,
       sum(o.item_subtotal_brl) as delivered_item_revenue_brl,
       max(to_date(o.order_purchase_timestamp)) as last_purchase_date
from {{ ref('int_orders_enriched') }} o
join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
where o.order_status = 'delivered'
group by c.customer_unique_id
