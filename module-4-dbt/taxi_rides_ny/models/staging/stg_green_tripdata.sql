{{ config(materialized='view') }}

WITH sourse AS (
    SELECT * FROM {{ source ('raw', 'green_tripdata') }}
),

renamed AS (
    SELECT
        -- identifiers
        CAST(VendorID AS INTEGER) AS vendor_id
        {{ safecast('RatecodeID', 'INTEGER' ) }} as rate_code_id,
        CAST(PULocationID AS INTEGER) AS pickup_location_id,
        CAST(DOLocationID AS INTEGER) AS dropoff_location_id,

        -- timestamps
        CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime, 
        CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        -- trip info
        CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
        CAST(passenger_count AS INTEGER) AS passenger_count,
        CAST(trip_distance AS NUMERIC) AS trip_distance,
        {{ safecast('trip_type', 'INTEGER') }} AS trip_type,

        -- payment info
        CAST(fare_amount AS NUMERIC) AS fare_amount,
        CAST(extra AS NUMERIC) AS extra,
        CAST(mta_tax AS NUMERIC) AS mta_tax,
        CAST(tip_ammount AS NUMERIC) AS tip_ammount,
        CAST(tolls_amount AS NUMERIC) AS tolls_amount,
        --CAST(ehail_fee AS NUMERIC) AS ehail_fee, Column provides no analytical value and is typically excluded from downstream models.
        CAST(payment_type AS INTEGER) AS payment_type -- Standard DE Zoomcamp course ignored this column, but I'm including it for tip-ratio analysis
        CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
        CAST(total_amount AS NUMERIC) AS total_amount,
        {{ safecast('payment_type','INTEGER') }} AS payment_type
    FROM source
    WHERE vendorid IS NOT NULL
)