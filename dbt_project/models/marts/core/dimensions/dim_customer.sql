select s.*, s.dbt_scd_id as customer_key,
       concat(s.customer_zip_code_prefix, '|', upper(s.customer_state)) as geography_key,
       row_number() over (partition by s.customer_id order by s.dbt_valid_from) as version_number
from {{ ref('customers_snapshot') }} s
