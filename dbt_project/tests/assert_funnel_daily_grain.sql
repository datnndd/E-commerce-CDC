select session_date_key, device, channel_key
from {{ ref('mart_funnel_daily') }}
group by session_date_key, device, channel_key
having count(*) > 1
