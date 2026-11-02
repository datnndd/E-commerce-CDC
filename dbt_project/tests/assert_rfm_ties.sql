with scores as (
    select customer_unique_id, r_score, f_score, m_score,
           min(r_score) over (partition by recency_days) as min_r,
           max(r_score) over (partition by recency_days) as max_r,
           min(f_score) over (partition by frequency_orders) as min_f,
           max(f_score) over (partition by frequency_orders) as max_f,
           min(m_score) over (partition by monetary_brl) as min_m,
           max(m_score) over (partition by monetary_brl) as max_m
    from {{ ref('mart_customer_rfm') }}
)
select customer_unique_id
from scores
where min_r <> max_r or min_f <> max_f or min_m <> max_m
