{#
    Union green and yellow taxi data into a single dataset
    Demonstrates how to combine data from multiple sources with slightly different schemas
#}

WITH green_tripdata AS (
    SELECT
        -- exclude loading information (unique_row_id, filename)
        vendor_id
        rate_code_id
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        payment_type,
        congestion_surcharge,
        total_amount,
        'Green' as taxi_type
    from {{ ref('stg_green_tripdata') }} 
),

yellow_trips AS (
    SELECT
        -- exclude loading information (unique_row_id, filename)
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        cast(1 as integer) as trip_type,  -- Yellow taxis only do street-hail (code 1)
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        payment_type,
        total_amount,
        'Yellow' as service_type
    FROM {{ ref('stg_yellow_tripdata') }}
)

SELECT * FROM green_trips
UNION ALL
SELECT * FROM yellow_trips