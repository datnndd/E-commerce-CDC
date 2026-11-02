select p.order_purchase_date_key, d.date_day as order_purchase_date,
       p.payment_type,
       count(*) as payment_record_count,
       count(distinct p.order_id) as paid_order_count,
       sum(p.payment_value_brl) as payment_value_brl,
       avg(p.payment_installments) as average_installments,
       sum(case when p.payment_installments > 1 then 1 else 0 end) as installment_payment_count
from {{ ref('fct_payments') }} p
join {{ ref('dim_date') }} d on p.order_purchase_date_key = d.date_key
group by p.order_purchase_date_key, d.date_day, p.payment_type
