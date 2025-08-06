{{ config(
    materialized='table',
    schema='raw_vpanchal'
) }}

WITH accounts AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'accounts') }}
)

SELECT
    *
FROM 
    accounts