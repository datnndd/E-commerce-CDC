select * from {{ source('silver', 'slv_exchange_rates') }}
