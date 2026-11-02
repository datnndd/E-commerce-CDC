select s.session_date_key, d.date_day as session_date,
       s.device, s.channel_key,
       count(*) as session_count,
       sum(s.event_count) as event_count,
       sum(s.viewed) as view_sessions,
       sum(s.added_to_cart) as cart_sessions,
       sum(s.began_checkout) as checkout_sessions,
       sum(s.purchased) as purchase_sessions,
       cast(sum(s.purchased) / count(*) as decimal(10,4)) as session_purchase_rate,
       cast(sum(s.added_to_cart) / nullif(sum(s.viewed), 0) as decimal(10,4)) as view_to_cart_rate,
       cast(sum(s.began_checkout) / nullif(sum(s.added_to_cart), 0) as decimal(10,4)) as cart_to_checkout_rate,
       cast(sum(s.purchased) / nullif(sum(s.began_checkout), 0) as decimal(10,4)) as checkout_to_purchase_rate
from {{ ref('fct_sessions') }} s
join {{ ref('dim_date') }} d on s.session_date_key = d.date_key
group by s.session_date_key, d.date_day, s.device, s.channel_key
