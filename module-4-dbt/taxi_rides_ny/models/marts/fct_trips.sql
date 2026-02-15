{{
    config(
        materialized='table'
    )
}}

WITH trips_data AS (
    SELECT * FROM {{ ref('int_trips_unioned') }}
), 

dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT 
    t.tripid, -- kestra generated MD5 hash of all the fields in the source data, to be used as a unique identifier for each trip
    t.vendor_id,
    t.service_type,
    t.rate_code_id,
    t.pickup_location_id,
    dz.borough AS pickup_borough,
    dz.zone AS pickup_zone,
    t.dropoff_location_id,
    dz2.borough AS dropoff_borough,
    dz2.zone AS dropoff_zone,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.store_and_fwd_flag,
    t.passenger_count,
    t.trip_distance,
    t.trip_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.ehail_fee,
    t.improvement_surcharge,
    t.total_amount,
    t.payment_type,
    {{ get_payment_type_description('t.payment_type') }} AS payment_type_description
FROM trips_data t
LEFT JOIN dim_zones dz ON t.pickup_location_id = dz.zone_id
LEFT JOIN dim_zones dz2 ON t.dropoff_location_id = dz2.zone_id