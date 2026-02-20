{#
    Fact tables are meant to store transactional data (e.g., trip details).
#}

{{
    config(
        materialized='incremental',
        inoque_key='trip_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

