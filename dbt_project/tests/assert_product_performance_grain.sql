select product_id, seller_id, category_key
from {{ ref('mart_product_performance') }}
group by product_id, seller_id, category_key
having count(*) > 1
