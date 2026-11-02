{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['date', 'base', 'quote'], file_format='delta') }}
select date, cast(date_format(date, 'yyyyMMdd') as int) as date_key,
       base, quote, rate, ingested_at as source_ingested_at
from {{ ref('stg_exchange_rates') }} r
{% if is_incremental() %}
where r.ingested_at >= (select coalesce(max(source_ingested_at), timestamp('1900-01-01')) from {{ this }})
  and not exists (select 1 from {{ this }} existing
                  where existing.date = r.date and existing.base = r.base and existing.quote = r.quote
                    and existing.source_ingested_at = r.ingested_at)
{% endif %}
