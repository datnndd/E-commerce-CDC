select purchase_date_key, customer_state
from {{ ref('mart_daily_sales') }}
group by purchase_date_key, customer_state
having count(*) > 1
