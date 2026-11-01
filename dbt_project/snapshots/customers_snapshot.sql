{% snapshot customers_snapshot %}
{{ config(unique_key='customer_id', strategy='timestamp', updated_at='updated_at', file_format='delta') }}
select * from {{ ref('stg_customers') }}
{% endsnapshot %}
