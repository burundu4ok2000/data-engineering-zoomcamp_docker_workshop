{{ config(materialized='table') }}

WITH taxi_zone_lookup AS (
    SELECT * FROM {{ ref('taxi_zone_lookup') }}
),

renamed_zones AS (
    SELECT
        locationid AS zone_id,
        borough,
        zone,
        service_zone
    FROM taxi_zone_lookup
)

SELECT * FROM renamed_zones