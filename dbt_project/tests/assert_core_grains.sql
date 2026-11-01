with duplicate_items as (
    select order_id, order_item_id from {{ ref('fct_order_items') }}
    group by order_id, order_item_id having count(*) > 1
), duplicate_payments as (
    select order_id, payment_sequential from {{ ref('fct_payments') }}
    group by order_id, payment_sequential having count(*) > 1
), duplicate_rates as (
    select date, base, quote from {{ ref('fct_exchange_rates') }}
    group by date, base, quote having count(*) > 1
)
select 'order_items' as relation_name from duplicate_items
union all select 'payments' from duplicate_payments
union all select 'exchange_rates' from duplicate_rates
