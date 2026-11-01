{% snapshot products_snapshot %}
{{ config(unique_key='product_id', strategy='timestamp', updated_at='updated_at', file_format='delta') }}
select * from {{ ref('stg_products') }}
{% endsnapshot %}
