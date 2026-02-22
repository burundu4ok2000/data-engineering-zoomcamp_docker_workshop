{#
    Dimension table does not change very often.
    By using dim_ tables we keep fct_ tables clean and free from non-transactional information
    dim_ tables are reusable across different fct_ tables
    When you query a fact table, dbt will automatically join with the `dim_vendors` table to get the actual names.
#}

WITH trips AS (
    SELECT * FROM {{ ref('fct_trips') }}
),

vendors AS (
    SELECT DISTINCT
        vendor_id,
        {{ get_vendor_data('vendor_id') }} AS vendor_name 
    FROM trips
)

SELECT * FROM vendors