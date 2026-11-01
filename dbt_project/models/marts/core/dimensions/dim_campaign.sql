select campaign_id as campaign_key, campaign_id, utm_source, discount_pct, synthetic
from {{ ref('stg_campaigns') }}
