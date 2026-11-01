with checks as (
    select 'orders' as relation_name,
           (select count(*) from {{ ref('stg_orders') }}) as source_count,
           (select count(*) from {{ ref('fct_orders') }}) as fact_count
    union all
    select 'order_items',
           (select count(*) from {{ ref('stg_order_items') }}),
           (select count(*) from {{ ref('fct_order_items') }})
    union all
    select 'payments',
           (select count(*) from {{ ref('stg_payments') }}),
           (select count(*) from {{ ref('fct_payments') }})
    union all
    select 'reviews',
           (select count(*) from {{ ref('stg_reviews') }}),
           (select count(*) from {{ ref('fct_reviews') }})
    union all
    select 'clickstream',
           (select count(*) from {{ ref('stg_clickstream_events') }}),
           (select count(*) from {{ ref('fct_clickstream_events') }})
    union all
    select 'exchange_rates',
           (select count(*) from {{ ref('stg_exchange_rates') }}),
           (select count(*) from {{ ref('fct_exchange_rates') }})
)
select * from checks where source_count <> fact_count
