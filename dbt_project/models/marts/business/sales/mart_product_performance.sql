select i.product_id, i.seller_id, i.category_key,
       c.category_name_english, s.seller_state,
       sum(i.item_count) as item_count,
       count(distinct i.order_id) as order_count,
       sum(i.item_price_brl) as delivered_item_revenue_brl,
       sum(i.freight_brl) as delivered_freight_brl,
       avg(i.item_price_brl) as average_item_price_brl
from {{ ref('fct_order_items') }} i
join {{ ref('dim_category') }} c on i.category_key = c.category_key
join {{ ref('dim_seller') }} s on i.seller_key = s.seller_key
where i.order_status = 'delivered'
group by i.product_id, i.seller_id, i.category_key,
         c.category_name_english, s.seller_state
