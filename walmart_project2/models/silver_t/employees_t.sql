{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}

WITH source_data AS (
    SELECT
        *,
        current_timestamp() AS processed_at
    FROM
        {{ source('walmart_databricks', 'employees') }}
   
)

SELECT *
FROM source_data
{% if is_incremental() %}
WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1970-01-01') FROM {{ this }})
{% endif %}