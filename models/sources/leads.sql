{{ config(
    materialized='table',
    schema='raw_vpanchal'
) }}

WITH leads AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'leads') }}
)

SELECT
    *
FROM 
    leads