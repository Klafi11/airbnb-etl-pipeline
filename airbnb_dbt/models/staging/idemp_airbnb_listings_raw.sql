{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['listing_id', 'snapshot_date']
) }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY listing_id, DATE(processed_at)
            ORDER BY processed_at DESC
        ) AS rn
    FROM {{ source('airbnb_staging', 'stg_airbnb_listings_raw') }}

    {% if is_incremental() %}
      WHERE processed_at >= (
          SELECT MAX(processed_at) FROM {{ this }}
      )
    {% endif %}
)

SELECT
    *
FROM ranked
WHERE rn = 1
