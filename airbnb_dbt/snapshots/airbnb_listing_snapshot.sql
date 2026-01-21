{% snapshot airbnb_listing_snapshot %}

{{
  config(
    unique_key='listing_id',
    strategy='check',
    check_cols=[
      'listing_name',
      'room_type',
      'price',
      'minimum_nights',
      'license'
    ]
  )
}}

select
  listing_id,
  listing_name,
  room_type,
  price,
  minimum_nights,
  longitude,
  latitude,
  license
from {{ ref('stg_airbnb_listings') }}
qualify row_number() over (
  partition by listing_id
  order by snapshot_date desc
) = 1

{% endsnapshot %}

