{% snapshot airbnb_host_snapshot %}

{{
  config(
    unique_key='host_id',
    strategy='check',
    check_cols=[
      'host_name',
      'calculated_host_listings_count'
    ]
  )
}}

select
  host_id,
  host_name,
  calculated_host_listings_count
from {{ ref('stg_airbnb_listings') }}
qualify row_number() over (
  partition by host_id
  order by
    snapshot_date desc,
    calculated_host_listings_count desc,
    host_name desc
) = 1

{% endsnapshot %}
