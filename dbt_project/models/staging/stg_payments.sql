select * from {{ source('silver', 'slv_payments') }}
