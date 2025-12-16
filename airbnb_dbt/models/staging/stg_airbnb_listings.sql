{{config(materialized="view")}}

 select
    -- identifiers
    cast(id as string)                      as listing_id,
    cast(name as string)                    as listing_name,

    cast(host_id as string)                 as host_id,
    cast(host_name as string)               as host_name,

    -- listing attributes
    cast(room_type as string)               as room_type,
    cast(price as numeric)                  as price,
    cast(minimum_nights as int64)            as minimum_nights,
    cast(latitude as numeric)               as latitude,
    cast(longitude as numeric)              as longitude,
    cast(license as string)                 as license,

    -- host attributes
    cast(calculated_host_listings_count
         as int64)                           as calculated_host_listings_count,

    -- geography
    cast(neighbourhood_group as string)     as borough,
    cast(neighbourhood as string)            as neighbourhood,

    -- snapshot metrics
    cast(availability_365 as int64)          as availability_365,
    cast(number_of_reviews as int64)         as number_of_reviews,
    cast(number_of_reviews_ltm as int64)     as number_of_reviews_ltm,

    -- snapshot date
    cast(processed_at as date)              as snapshot_date


FROM {{ref('idemp_airbnb_listings_raw')}}
