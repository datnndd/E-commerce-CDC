{{ config(materialized='incremental', incremental_strategy='merge', unique_key='event_id', file_format='delta') }}
select e.event_id, e.session_id, e.event_type, e.event_timestamp,
       cast(date_format(from_utc_timestamp(e.event_timestamp, 'America/Sao_Paulo'), 'yyyyMMdd') as int) as event_date_key,
       e.customer_id, c.customer_key, e.product_id, p.product_key,
       e.order_id, e.device, coalesce(e.utm_source, 'unknown') as channel_key,
       e.ingested_at as source_ingested_at
from {{ ref('stg_clickstream_events') }} e
left join {{ ref('dim_customer') }} c
  on e.customer_id = c.customer_id
 and ((e.event_timestamp >= c.dbt_valid_from
       and (c.dbt_valid_to is null or e.event_timestamp < c.dbt_valid_to))
      or (c.version_number = 1 and e.event_timestamp < c.dbt_valid_from))
left join {{ ref('dim_product') }} p
  on e.product_id = p.product_id
 and ((e.event_timestamp >= p.dbt_valid_from
       and (p.dbt_valid_to is null or e.event_timestamp < p.dbt_valid_to))
      or (p.version_number = 1 and e.event_timestamp < p.dbt_valid_from))
{% if is_incremental() %}
where e.ingested_at >= (select coalesce(max(source_ingested_at), timestamp('1900-01-01')) from {{ this }})
  and not exists (select 1 from {{ this }} existing
                  where existing.event_id = e.event_id
                    and existing.source_ingested_at = e.ingested_at)
{% endif %}
