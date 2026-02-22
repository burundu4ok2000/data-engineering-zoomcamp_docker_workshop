{#
    Data mart for monthly revenue analysis by pickup zone and service type
    This aggregation is optimized for business reporting and dashboards
    Enables analysis of revenue trends across different zones and taxi types

    TODOs:
    1. Where are the filtration of the bad data? (trip_distance, passenger_count) - at staging
    2. Do I need to filter bad data in Payment rows? (I think no)
    3. Do I need to specify data_tests in schema.yml? For example passenger_count < 6 && passenger_count > 0
    4. Add descriptions to schema.yml file. 
#}

SELECT
    -- Grouping dimensions
    COALESCE(pickup_zone, 'Unknown Zone') AS pickup_zone,
    
    {% if target.type == 'bigquery' %} 
        CAST(date_trunc(pickup_datetime, MONTH) AS DATE)
    {% elif target.type == 'duckdb' %}
        date_trunc('month', pickup_datetime) 
    {% endif %} AS revenue_month,
    
    service_type, 

    -- Revenue breakdown (summed by zone, month, and service type)
    SUM(fare_amount) AS revenue_monthly_fare_amount,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional metrics for operational analysis
    COUNT(trip_id) AS total_monthly_trips,
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance,

FROM {{ ref('fct_trips') }}
GROUP BY pickup_zone, revenue_month, service_type
