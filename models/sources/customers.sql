{{ config(
    materialized='table',
    schema='raw_vpanchal'
) }}

WITH customers AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'customers') }}
)

SELECT
    *
FROM 
    customers