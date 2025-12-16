select
    dbt_scd_id as host_sk,
    host_id,
    host_name,
    calculated_host_listings_count,

    cast(dbt_valid_from as date) as effective_from,
    cast(dbt_valid_to as date) as effective_to,
    dbt_valid_to is null as is_current
    
from {{ ref('airbnb_host_snapshot') }}
