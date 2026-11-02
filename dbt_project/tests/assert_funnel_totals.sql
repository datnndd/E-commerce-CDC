with mart as (
    select sum(session_count) as sessions
    from {{ ref('mart_funnel_daily') }}
), fact as (
    select count(*) as sessions from {{ ref('fct_sessions') }}
)
select mart.sessions, fact.sessions
from mart cross join fact
where mart.sessions <> fact.sessions
