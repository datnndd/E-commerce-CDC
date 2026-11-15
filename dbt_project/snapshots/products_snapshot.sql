{% snapshot products_snapshot %}
{{ config(unique_key='product_id', strategy='timestamp',
          updated_at='ingested_at' if env_var('CDC_MODE', 'polling') == 'debezium' else 'updated_at',
          file_format='delta', hard_deletes='invalidate') }}
select * from {{ ref('stg_products') }}
{% endsnapshot %}
