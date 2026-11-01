select * from {{ source('silver', 'slv_clickstream_events') }}
