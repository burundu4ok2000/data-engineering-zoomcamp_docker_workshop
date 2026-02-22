{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'green_tripdata') }}
    WHERE vendorid IS NOT NULL 
),

renamed AS (
    SELECT
        -- loading information
        CAST(unique_row_id AS BYTES) AS unique_row_id,
        CAST(filename AS STRING) AS file_name,

        -- identifiers
        CAST(VendorID AS INTEGER) AS vendor_id,
        SAFE_CAST(RatecodeID AS INT64) AS rate_code_id,
        SAFE_CAST(PULocationID AS INT64) AS pickup_location_id,
        SAFE_CAST(DOLocationID AS INT64) AS dropoff_location_id,

        -- timestamps
        CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime, 
        CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        -- trip info
        CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
        CAST(passenger_count AS INTEGER) AS passenger_count,
        CAST(trip_distance AS NUMERIC) AS trip_distance,
        SAFE_CAST(trip_type AS INT64) AS trip_type,

        -- payment info
        CAST(fare_amount AS NUMERIC) AS fare_amount,
        CAST(extra AS NUMERIC) AS extra,
        CAST(mta_tax AS NUMERIC) AS mta_tax,
        CAST(tip_amount AS NUMERIC) AS tip_amount,
        CAST(tolls_amount AS NUMERIC) AS tolls_amount,
        CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
        CAST(total_amount AS NUMERIC) AS total_amount,
        SAFE_CAST(payment_type AS INT64) AS payment_type,
        CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge
    FROM source
)

SELECT * FROM renamed
WHERE passenger_count BETWEEN 0 AND 6
  AND trip_distance >= 0 AND trip_distance <= 50 
  AND dropoff_datetime > pickup_datetime
  AND pickup_location_id BETWEEN 1 AND 265
  AND dropoff_location_id BETWEEN 1 AND 265
  AND total_amount >= 2.50 AND total_amount <= 500
  AND fare_amount > 0 
  AND payment_type IN (1, 2, 3, 4, 6) 
  AND rate_code_id <= 6
  AND mta_tax IN (0, 0.5)
  AND extra IN (0, 0.5, 1.0, 2.5, 3.0, 3.5)
  AND FORMAT_TIMESTAMP('%Y-%m', pickup_datetime) = REGEXP_EXTRACT(file_name, r'\d{4}-\d{2}')

-- Limit data volume in development to reduce BigQuery scan costs and speed up execution
{% if target.name == 'dev' %}
  AND pickup_datetime >= '2019-01-01' AND pickup_datetime < '2019-02-01'
{% endif %}