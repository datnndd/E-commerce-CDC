select order_id, order_item_id, item_price_brl, freight_brl
from {{ ref('fct_order_items') }}
where item_price_brl < 0 or freight_brl < 0
