{#
    In BigQuery, `safe_cast` is a special kind of casting function that helps 
    prevent errors when you try to convert data from one type to another.

    Imagine you have a column with mixed types

    ```sql
    SELECT CAST('123', 'hello' AS INT64)
    ```

    This would give you:

    - `123`
    - An error (`Invalid literal hello for cast to INT64 at [1:7]`)

    Now, if we use `SAFE_CAST`, it will try its best and return `NULL` instead of 
    causing an error when the conversion fails.

    ```sql
    SELECT SAFE_CAST('123','hello' AS INT64)
    ```

    This would give you:

    - `123`
    - `NULL`

    Using `safe_cast` is a good practice when you are unsure about the data types 
    in your columns or want to avoid errors that can stop your entire query from 
    running.
#}

{% macro_safecast(column, datetype) %}

    {% if target.type == 'bigquery' %}
        safecast({{ column }}, {{ datetype }})
    {% else %}
        cast({{ column }}, {{ datetype }})
    {% endif %}

{% endmacro %}