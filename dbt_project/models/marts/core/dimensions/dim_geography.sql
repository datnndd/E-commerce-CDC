with locations as (
    select customer_zip_code_prefix as zip_prefix, upper(customer_state) as state
    from {{ ref('customers_snapshot') }}
    union
    select seller_zip_code_prefix as zip_prefix, upper(seller_state) as state
    from {{ ref('stg_sellers') }}
), coordinates as (
    select geolocation_zip_code_prefix as zip_prefix, upper(geolocation_state) as state,
           avg(geolocation_lat) as latitude, avg(geolocation_lng) as longitude,
           count(distinct geolocation_city) as city_variant_count
    from {{ ref('stg_geolocation') }}
    group by geolocation_zip_code_prefix, upper(geolocation_state)
)
select concat(l.zip_prefix, '|', l.state) as geography_key,
       l.zip_prefix, l.state, c.latitude, c.longitude,
       coalesce(c.city_variant_count, 0) as city_variant_count
from locations l
left join coordinates c on l.zip_prefix = c.zip_prefix and l.state = c.state
