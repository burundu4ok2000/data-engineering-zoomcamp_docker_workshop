SELECT 
    -- identifiers
    unique_row_id as tripid,
    CAST(vendorid as int64) as vendor_id,
    'yellow' AS service_type,
    CAST(ratecodeid as int64) as rate_code_id,
    CAST(pulocationid as int64) as pickup_location_id, 
    CAST(dolocationid as int64) as dropoff_location_id, 

    -- timestamps
    CAST(tpep_pickup_datetime as timestamp) as pickup_datetime, 
    CAST(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip details
    store_and_fwd_flag, 
    CAST(passenger_count as int64) as passenger_count, 
    CAST(trip_distance as float64) as trip_distance, 
    -- Yellow taxi data doesn't have trip_type, because it's always street-hail (1).
    CAST(1 as int64) as trip_type, 

    -- payment details
    CAST(fare_amount as numeric) as fare_amount,
    CAST(extra as numeric) as extra,
    CAST(mta_tax as numeric) as mta_tax,
    CAST(tip_amount as numeric) as tip_amount,
    CAST(tolls_amount as numeric) as tolls_amount,
    -- Yellow taxi data doesn't have ehail_fee.
    CAST(0 as numeric) as ehail_fee, 
    CAST(improvement_surcharge as numeric) as improvement_surcharge,
    CAST(total_amount as numeric) as total_amount,
    CAST(payment_type as int64) as payment_type

FROM {{ source('raw_data', 'yellow_tripdata') }}
WHERE vendorid IS NOT NULL