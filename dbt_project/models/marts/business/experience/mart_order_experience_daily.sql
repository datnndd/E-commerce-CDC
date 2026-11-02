with reviews_by_order as (
    select order_id, avg(cast(review_score as double)) as order_review_score
    from {{ ref('fct_reviews') }}
    group by order_id
), delivered as (
    select o.purchase_date_key, coalesce(g.state, 'UNKNOWN') as customer_state,
           o.order_id, r.order_review_score,
           datediff(
               to_date(from_utc_timestamp(o.order_delivered_customer_date, 'America/Sao_Paulo')),
               to_date(from_utc_timestamp(o.order_purchase_timestamp, 'America/Sao_Paulo'))
           ) as delivery_days,
           case when to_date(from_utc_timestamp(o.order_delivered_customer_date, 'America/Sao_Paulo'))
                       > to_date(from_utc_timestamp(o.order_estimated_delivery_date, 'America/Sao_Paulo'))
                then 1 else 0 end as late_delivery
    from {{ ref('fct_orders') }} o
    left join {{ ref('dim_geography') }} g
      on o.customer_geography_key = g.geography_key
    left join reviews_by_order r on o.order_id = r.order_id
    where o.order_status = 'delivered'
)
select o.purchase_date_key, d.date_day as purchase_date, o.customer_state,
       count(*) as delivered_order_count,
       count(o.delivery_days) as orders_with_delivery_date,
       sum(o.late_delivery) as late_delivery_order_count,
       cast(sum(o.late_delivery) / nullif(count(o.delivery_days), 0) as decimal(10,4)) as late_delivery_rate,
       avg(o.delivery_days) as average_delivery_days,
       count(o.order_review_score) as reviewed_order_count,
       avg(o.order_review_score) as average_order_review_score
from delivered o
join {{ ref('dim_date') }} d on o.purchase_date_key = d.date_key
group by o.purchase_date_key, d.date_day, o.customer_state
