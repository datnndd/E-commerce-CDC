with mart as (
    select sum(payment_record_count) as payments, sum(payment_value_brl) as value_brl
    from {{ ref('mart_payment_mix_daily') }}
), fact as (
    select count(*) as payments, sum(payment_value_brl) as value_brl
    from {{ ref('fct_payments') }}
)
select mart.payments, fact.payments, mart.value_brl, fact.value_brl
from mart cross join fact
where mart.payments <> fact.payments or mart.value_brl <> fact.value_brl
