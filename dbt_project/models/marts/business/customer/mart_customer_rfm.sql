with activity as (
    select customer_unique_id,
           count(*) as frequency_orders,
           sum(item_subtotal_brl) as monetary_brl,
           max(to_date(cast(purchase_date_key as string), 'yyyyMMdd')) as last_purchase_date
    from {{ ref('fct_orders') }}
    where order_status = 'delivered'
    group by customer_unique_id
), anchor as (
    select date_add(max(last_purchase_date), 1) as as_of_date from activity
), recency as (
    select a.*, x.as_of_date,
           datediff(x.as_of_date, a.last_purchase_date) as recency_days
    from activity a cross join anchor x
), scored as (
    select *,
           cast(least(5, floor(percent_rank() over (order by recency_days desc) * 5) + 1) as int) as r_score,
           cast(least(5, floor(percent_rank() over (order by frequency_orders) * 5) + 1) as int) as f_score,
           cast(least(5, floor(percent_rank() over (order by monetary_brl) * 5) + 1) as int) as m_score
    from recency
)
select customer_unique_id, as_of_date, last_purchase_date,
       recency_days, frequency_orders, monetary_brl,
       r_score, f_score, m_score,
       case
           when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
           when r_score >= 3 and f_score >= 4 then 'Loyal'
           when r_score >= 4 then 'Potential'
           when r_score >= 2 then 'At Risk'
           else 'Churned'
       end as segment
from scored
