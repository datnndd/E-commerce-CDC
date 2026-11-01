select seller_id as seller_key, seller_id, seller_zip_code_prefix, seller_city, seller_state,
       concat(seller_zip_code_prefix, '|', upper(seller_state)) as geography_key
from {{ ref('stg_sellers') }}
