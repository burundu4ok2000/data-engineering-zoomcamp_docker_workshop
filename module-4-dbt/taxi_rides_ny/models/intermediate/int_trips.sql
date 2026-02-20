WITH unioned_trips AS (
    SELECT * FROM {{ ref('int_trips_unioned') }}
),

payment_types AS (
    SELECT * FROM {{ ref('payment_type_lookup') }}
),

cleaned_and_enriched AS (
    SELECT
        -- Generate unique trip identifier (surrogate key pattern)
        {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.pickup_location_id', 'u.service_type']) }} AS trip_id,

        -- identifiers
        u.vendor_id,
        u.service_type,
        u.rate_code_id,

        -- Location IDs
        u.pickup_location_id,
        u.dropoff_location_id,

        -- Timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        -- Trip details
        u.store_and_fwd_flag,
        u.passenger_count,
        u.trip_distance,
        u.trip_type,

        -- Payment breakdown
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.improvement_surcharge,
        u.congestion_surcharge,
        u.total_amount,

        -- Enrich with payment type description
        COALESCE(u.payment_type, 0) AS payment_type,
        COALESCE(pt.description, 'Unknown') AS payment_type_description

    FROM unioned u
    LEFT JOIN payment_types pt
        ON COALESCE(u.payment_type, 0) = pt.payment_type
)

SELECT * FROM cleaned_and_enriched

-- Deduplicate: if multiple trips match (same vendor, second, location, service), keep first
-- WINDOW FUNCTION OVER() -
qualify row_number() OVER(
    PARTITION BY vendor_id, pickup_datetime, pickup_location_id, service_type
    ORDER BY dropoff_datetime
) = 1
