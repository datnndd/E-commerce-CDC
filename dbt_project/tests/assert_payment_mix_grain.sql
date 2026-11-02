select order_purchase_date_key, payment_type
from {{ ref('mart_payment_mix_daily') }}
group by order_purchase_date_key, payment_type
having count(*) > 1
