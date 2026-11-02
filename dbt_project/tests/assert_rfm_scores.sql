select customer_unique_id
from {{ ref('mart_customer_rfm') }}
where r_score not between 1 and 5
   or f_score not between 1 and 5
   or m_score not between 1 and 5
   or recency_days <> datediff(as_of_date, last_purchase_date)
   or segment <> case
       when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
       when r_score >= 3 and f_score >= 4 then 'Loyal'
       when r_score >= 4 then 'Potential'
       when r_score >= 2 then 'At Risk'
       else 'Churned'
   end
