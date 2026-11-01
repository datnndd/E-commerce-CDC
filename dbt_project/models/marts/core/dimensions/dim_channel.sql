with names as (
    select utm_source from {{ ref('stg_clickstream_events') }} where utm_source is not null
    union
    select utm_source from {{ ref('stg_campaigns') }} where utm_source is not null
    union
    select 'unknown' as utm_source
)
select utm_source as channel_key, utm_source
from names
