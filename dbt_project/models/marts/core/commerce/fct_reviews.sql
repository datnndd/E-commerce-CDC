{{ config(materialized='incremental', incremental_strategy='merge', unique_key='review_row_id', file_format='delta') }}
select r.review_row_id, r.review_id, r.order_id, o.customer_id, o.customer_unique_id,
       o.customer_key, o.purchase_date_key as order_purchase_date_key,
       cast(date_format(from_utc_timestamp(r.review_creation_date, 'America/Sao_Paulo'), 'yyyyMMdd') as int) as review_date_key,
       r.review_score, r.review_creation_date, r.review_answer_timestamp,
       (unix_timestamp(r.review_answer_timestamp) - unix_timestamp(r.review_creation_date)) / 3600.0 as response_hours,
       greatest(r.ingested_at, o.source_ingested_at) as source_ingested_at
from {{ ref('stg_reviews') }} r
join {{ ref('fct_orders') }} o on r.order_id = o.order_id
{% if is_incremental() %}
where greatest(r.ingested_at, o.source_ingested_at) >=
      (select coalesce(max(source_ingested_at), timestamp('1900-01-01')) from {{ this }})
  and not exists (select 1 from {{ this }} existing
                  where existing.review_row_id = r.review_row_id
                    and existing.source_ingested_at = greatest(r.ingested_at, o.source_ingested_at))
{% endif %}
