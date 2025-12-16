-- Replace YOUR_PROJECT_ID and DATASET_NAME before running
CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.${DATASET_STAGING}`;

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET_STAGING}.stg_airbnb_listings_raw` (
  id INT64 NOT NULL,
  name STRING NOT NULL,
  host_id INT64 NOT NULL,
  host_name STRING NOT NULL,
  neighbourhood_group STRING NOT NULL,
  neighbourhood STRING NOT NULL,
  latitude FLOAT64 NOT NULL,
  longitude FLOAT64 NOT NULL,
  room_type STRING NOT NULL,
  price FLOAT64 NOT NULL,
  minimum_nights INT64 NOT NULL,
  number_of_reviews INT64 NOT NULL,
  last_review DATE NOT NULL,
  reviews_per_month FLOAT64 NOT NULL,
  calculated_host_listings_count INT64 NOT NULL,
  availability_365 INT64 NOT NULL,
  number_of_reviews_ltm INT64 NOT NULL,
  license STRING NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  _record_id STRING NOT NULL,
)
PARTITION BY DATE(processed_at)
CLUSTER BY id;
