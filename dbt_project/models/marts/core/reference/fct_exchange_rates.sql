{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['date', 'base', 'quote'], file_format='delta') }}
select date, cast(date_format(date, 'yyyyMMdd') as int) as date_key,
       base, quote, rate, ingested_at as source_ingested_at
from {{ ref('stg_exchange_rates') }}
{% if is_incremental() %}
where ingested_at >= (select coalesce(max(source_ingested_at), timestamp('1900-01-01')) from {{ this }})
{% endif %}
