{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

WITH source_data AS (
    SELECT
        *,
        current_timestamp() AS processed_at
    FROM
        {{ source('walmart_databricks', 'orders') }}
  
)

SELECT *
FROM source_data
{% if is_incremental() %}
WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1970-01-01') FROM {{ this }})
{% endif %}