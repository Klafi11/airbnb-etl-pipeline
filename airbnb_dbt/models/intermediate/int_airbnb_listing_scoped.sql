select
    s.snapshot_date,

    dl.listing_sk,
    dh.host_sk,
    dn.neighbourhood_sk,

    -- ✅ FACT METRICS ONLY
    s.availability_365,
    s.number_of_reviews,
    s.number_of_reviews_ltm
from {{ ref('stg_airbnb_listings') }} s

join {{ ref('dim_listing_scd2') }} dl
  on s.listing_id = dl.listing_id
 and s.snapshot_date >= dl.effective_from
                        and s.snapshot_date < coalesce(dl.effective_to, date '9999-12-31')

join {{ ref('dim_host_scd2') }} dh
  on s.host_id = dh.host_id
 and s.snapshot_date >= dh.effective_from and
                        s.snapshot_date < coalesce(dh.effective_to, date '9999-12-31')

join {{ ref('dim_neighbourhood') }} dn
  on s.borough = dn.borough
 and s.neighbourhood = dn.neighbourhood
