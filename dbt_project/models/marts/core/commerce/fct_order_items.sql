{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['order_id', 'order_item_id'], file_format='delta') }}
select i.order_id, i.order_item_id, i.product_id, i.seller_id,
       o.customer_id, o.customer_unique_id, o.customer_key,
       o.customer_geography_key, s.geography_key as seller_geography_key,
       p.product_key, p.category_key, s.seller_key,
       o.purchase_date_key, o.order_purchase_timestamp, o.order_status,
       i.shipping_limit_date, i.price as item_price_brl,
       i.freight_value as freight_brl, 1 as item_count,
       greatest(i.ingested_at, o.source_ingested_at) as source_ingested_at
from {{ ref('stg_order_items') }} i
join {{ ref('fct_orders') }} o on i.order_id = o.order_id
join {{ ref('dim_product') }} p
  on i.product_id = p.product_id
 and ((o.order_purchase_timestamp >= p.dbt_valid_from
       and (p.dbt_valid_to is null or o.order_purchase_timestamp < p.dbt_valid_to))
      or (p.version_number = 1 and o.order_purchase_timestamp < p.dbt_valid_from))
join {{ ref('dim_seller') }} s on i.seller_id = s.seller_id
{% if is_incremental() %}
where greatest(i.ingested_at, o.source_ingested_at) >=
      (select coalesce(max(source_ingested_at), timestamp('1900-01-01')) from {{ this }})
{% endif %}
