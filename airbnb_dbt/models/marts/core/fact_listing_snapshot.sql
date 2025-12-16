{{ config(
    materialized='incremental',
    unique_key='listing_snapshot_sk'
) }}

select
    {{ dbt_utils.generate_surrogate_key(
        ['listing_sk','snapshot_date']
    ) }} as listing_snapshot_sk,

    listing_sk,
    host_sk,
    neighbourhood_sk,
 
    snapshot_date,
    availability_365,
    number_of_reviews,
    number_of_reviews_ltm
from {{ ref('int_airbnb_listing_scoped') }}

{% if is_incremental() %}
where snapshot_date >
      (select coalesce(max(snapshot_date), '1900-01-01') from {{ this }})
{% endif %}

