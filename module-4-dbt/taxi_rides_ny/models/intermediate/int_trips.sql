WITH unioned_trips AS (
    SELECT * FROM {{ ref('int_trips_unioned') }}
),

payment_types AS (
    SELECT * FROM {{ ref('payment_type_lookup') }}
),

cleaned_and_enriched AS (
    {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.pickup_location_id', 'u.service_type']) }} AS trip_is
)