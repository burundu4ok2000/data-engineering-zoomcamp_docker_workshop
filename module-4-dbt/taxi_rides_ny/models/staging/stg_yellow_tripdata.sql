WITH source AS (
    SELECT * FROM {{ source('raw', 'yellow_tripdata') }}
),

renamed AS (
    SELECT
        -- loading information
        CAST(unique_row_id AS BYTES) AS unique_row_id
        CAST(filename AS STRING) AS file_name

        -- identifiers
        CAST(VendorID AS INTEGER) AS vendor_id
        {{ safecast('RatecodeID', 'INTEGER') }} as rate_code_id,
        {{ safecast('PULocationID', 'INTEGER') }} AS pickup_location_id,
        {{ safecast('DOLocationID', 'INTEGER') }} AS dropoff_location_id,

        -- timestamps
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime, 
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        -- trip info
        CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
        CAST(passenger_count AS INTEGER) AS passenger_count,
        CAST(trip_distance AS NUMERIC) AS trip_distance,
        -- yellow_taxi_tripdata doesn't have trip_type column

        -- payment info
        CAST(fare_amount AS NUMERIC) AS fare_amount,
        CAST(extra AS NUMERIC) AS extra,
        CAST(mta_tax AS NUMERIC) AS mta_tax,
        CAST(tip_ammount AS NUMERIC) AS tip_ammount,
        CAST(tolls_amount AS NUMERIC) AS tolls_amount,
        -- yellow_taxi_tripdata doesn't have ehail_fee column
        CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
        CAST(total_amount AS NUMERIC) AS total_amount,
        {{ safecast('payment_type','INTEGER') }} AS payment_type
        CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge -- Standard DE Zoomcamp course ignored this column, but I'm including it
    FROM source
    WHERE vendor_id IS NOT NULL -- filter all of "Dispatch" records originating from HVFHV bases
)

SELECT * AS renamed

{% if target.name == 'dev' %}
    WHERE pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}