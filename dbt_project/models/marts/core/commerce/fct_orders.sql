{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_id', file_format='delta') }}
select o.order_id, o.customer_id, c.customer_unique_id, c.customer_key,
       c.geography_key as customer_geography_key,
       o.order_status, o.order_purchase_timestamp, o.order_approved_at,
       o.order_delivered_carrier_date, o.order_delivered_customer_date,
       o.order_estimated_delivery_date,
       cast(date_format(from_utc_timestamp(o.order_purchase_timestamp, 'America/Sao_Paulo'), 'yyyyMMdd') as int) as purchase_date_key,
       cast(date_format(from_utc_timestamp(o.order_delivered_customer_date, 'America/Sao_Paulo'), 'yyyyMMdd') as int) as delivery_date_key,
       1 as order_count, o.item_count, o.payment_count,
       o.item_subtotal_brl, o.freight_total_brl, o.payment_total_brl,
       o.source_ingested_at
from {{ ref('int_orders_enriched') }} o
join {{ ref('dim_customer') }} c
  on o.customer_id = c.customer_id
 and ((o.order_purchase_timestamp >= c.dbt_valid_from
       and (c.dbt_valid_to is null or o.order_purchase_timestamp < c.dbt_valid_to))
      or (c.version_number = 1 and o.order_purchase_timestamp < c.dbt_valid_from))
{% if is_incremental() %}
where o.source_ingested_at >= (select coalesce(max(source_ingested_at), timestamp('1900-01-01')) from {{ this }})
  and not exists (select 1 from {{ this }} existing
                  where existing.order_id = o.order_id
                    and existing.source_ingested_at = o.source_ingested_at)
{% endif %}
