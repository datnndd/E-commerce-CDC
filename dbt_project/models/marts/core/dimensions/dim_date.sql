with used_dates as (
    select to_date(from_utc_timestamp(order_purchase_timestamp, 'America/Sao_Paulo')) as day from {{ ref('stg_orders') }}
    union all select to_date(from_utc_timestamp(order_approved_at, 'America/Sao_Paulo')) from {{ ref('stg_orders') }}
    union all select to_date(from_utc_timestamp(order_delivered_customer_date, 'America/Sao_Paulo')) from {{ ref('stg_orders') }}
    union all select to_date(from_utc_timestamp(order_estimated_delivery_date, 'America/Sao_Paulo')) from {{ ref('stg_orders') }}
    union all select to_date(from_utc_timestamp(review_creation_date, 'America/Sao_Paulo')) from {{ ref('stg_reviews') }}
    union all select to_date(from_utc_timestamp(event_timestamp, 'America/Sao_Paulo')) from {{ ref('stg_clickstream_events') }}
    union all select date from {{ ref('stg_exchange_rates') }}
), bounds as (
    select min(day) as first_day, max(day) as last_day from used_dates
)
select cast(date_format(day, 'yyyyMMdd') as int) as date_key, day as date_day,
       year(day) as year, quarter(day) as quarter, month(day) as month,
       weekofyear(day) as week_of_year, dayofmonth(day) as day_of_month
from bounds lateral view explode(sequence(first_day, last_day, interval 1 day)) d as day
