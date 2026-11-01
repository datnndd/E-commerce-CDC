select * from {{ source('silver', 'slv_order_items') }}
