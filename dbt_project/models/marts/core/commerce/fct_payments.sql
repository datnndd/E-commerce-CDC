{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['order_id', 'payment_sequential'], file_format='delta') }}
select p.order_id, p.payment_sequential, o.customer_id, o.customer_unique_id,
       o.customer_key, o.purchase_date_key as order_purchase_date_key,
       p.payment_type, p.payment_installments, p.payment_value as payment_value_brl,
       greatest(p.ingested_at, o.source_ingested_at) as source_ingested_at
from {{ ref('stg_payments') }} p
join {{ ref('fct_orders') }} o on p.order_id = o.order_id
{% if is_incremental() %}
where greatest(p.ingested_at, o.source_ingested_at) >=
      (select coalesce(max(source_ingested_at), timestamp('1900-01-01')) from {{ this }})
{% endif %}
