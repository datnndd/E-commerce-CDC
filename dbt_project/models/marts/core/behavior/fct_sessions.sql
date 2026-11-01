select session_id,
       min(event_timestamp) as session_started_at,
       max(event_timestamp) as session_ended_at,
       cast(date_format(from_utc_timestamp(min(event_timestamp), 'America/Sao_Paulo'), 'yyyyMMdd') as int) as session_date_key,
       min_by(customer_id, event_timestamp) as customer_id,
       min_by(customer_key, event_timestamp) as customer_key,
       min_by(device, event_timestamp) as device,
       min_by(channel_key, event_timestamp) as channel_key,
       max(case when event_type = 'purchase' then order_id end) as purchased_order_id,
       count(*) as event_count,
       max(case when event_type = 'view_product' then 1 else 0 end) as viewed,
       max(case when event_type = 'add_to_cart' then 1 else 0 end) as added_to_cart,
       max(case when event_type = 'begin_checkout' then 1 else 0 end) as began_checkout,
       max(case when event_type = 'purchase' then 1 else 0 end) as purchased
from {{ ref('fct_clickstream_events') }}
group by session_id
