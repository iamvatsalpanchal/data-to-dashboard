{{ config(
    materialized='table',
    schema='raw_vpanchal'
) }}

WITH users AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'users') }}
)

SELECT
    *
FROM 
    users