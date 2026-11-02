select purchase_date_key, customer_state
from {{ ref('mart_order_experience_daily') }}
group by purchase_date_key, customer_state
having count(*) > 1
