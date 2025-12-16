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
    distinct
      host_id,
      host_name,
      calculated_host_listings_count,
from {{ ref('stg_airbnb_listings') }}

{% endsnapshot %}
