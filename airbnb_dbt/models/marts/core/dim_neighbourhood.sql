select distinct
    {{ dbt_utils.generate_surrogate_key(
        ['borough','neighbourhood']
    ) }} as neighbourhood_sk,
    borough,
    neighbourhood
from {{ ref('stg_airbnb_listings') }}
