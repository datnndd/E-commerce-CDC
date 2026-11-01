with items as (
    select order_id, count(*) as item_count,
           sum(price) as item_subtotal_brl,
           sum(freight_value) as freight_total_brl,
           max(ingested_at) as items_ingested_at
    from {{ ref('stg_order_items') }}
    group by order_id
), payments as (
    select order_id, count(*) as payment_count,
           sum(payment_value) as payment_total_brl,
           max(ingested_at) as payments_ingested_at
    from {{ ref('stg_payments') }}
    group by order_id
)
select o.*, coalesce(i.item_count, 0) as item_count,
       coalesce(i.item_subtotal_brl, cast(0 as decimal(18,2))) as item_subtotal_brl,
       coalesce(i.freight_total_brl, cast(0 as decimal(18,2))) as freight_total_brl,
       coalesce(p.payment_count, 0) as payment_count,
       coalesce(p.payment_total_brl, cast(0 as decimal(18,2))) as payment_total_brl,
       greatest(o.ingested_at, i.items_ingested_at, p.payments_ingested_at) as source_ingested_at
from {{ ref('stg_orders') }} o
left join items i on o.order_id = i.order_id
left join payments p on o.order_id = p.order_id
