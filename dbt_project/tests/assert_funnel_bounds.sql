select session_date_key, device, channel_key
from {{ ref('mart_funnel_daily') }}
where view_sessions > session_count
   or cart_sessions > view_sessions
   or checkout_sessions > cart_sessions
   or purchase_sessions > checkout_sessions
   or session_purchase_rate < 0 or session_purchase_rate > 1
