{{
    config(
        materialized='table'
    )
}}

{% if should_full_refresh() %}
    -- take all loads when full refresh
    SELECT load_id, schema_name FROM {{ source('dlt', '_dlt_loads') }}
    -- TODO: the status value must be configurable so we can chain packages
    WHERE status = 0
{% else %}
    -- take only loads with status = 0 and no other records
    SELECT load_id, schema_name FROM {{ source('dlt', '_dlt_loads') }}
    GROUP BY 1, 2
    -- not that it is a hack
    HAVING SUM(status) = 0
{% endif %}