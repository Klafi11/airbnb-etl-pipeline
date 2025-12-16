select
    dbt_scd_id as listing_sk,
    listing_id,
    listing_name,
    room_type,
    price,
    minimum_nights,
    longitude,
    latitude,
    license,
    
    cast(dbt_valid_from as date) as effective_from,
    cast(dbt_valid_to as date) as effective_to,
    dbt_valid_to is null as is_current
    
from {{ ref('airbnb_listing_snapshot') }}




