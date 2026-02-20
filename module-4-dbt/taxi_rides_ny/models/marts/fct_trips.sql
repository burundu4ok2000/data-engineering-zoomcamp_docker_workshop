{#
    Fact tables are meant to store transactional data (e.g., trip details).
    Fact table containing all taxi trips with zone information
    This is a classic star schema design: fact table (trips) joined to dimension table (zones)
    Materialized incrementally to handle large datasets efficiently
#}

{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

SELECT
    --trip identifiers
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

     -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    {{ get_trip_duration_minutes('trips.pickup_datetime', 'trips.dropoff_datetime') }} as trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.congestion_surcharge,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

FROM {{ ref('int_trips') }} AS trips
-- LEFT JOIN preserves all trips even if zone information is missing or unknown
LEFT JOIN {{ ref('dim_zones') }} AS pz
    ON trips.pickup_location_id = pz.location_id
LEFT JOIN {{ ref('dim_zones') }} AS dz
    ON trips.dropoff_location_id = dz.location_id

{% if is_incremental() %}
    -- Only process new trips based on pickup datetime
    WHERE trips.pickup_datetime > (SELECT MAX(pickup_datetime) FROM{{ this }})
{% endif %}
 

