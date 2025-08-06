{{ config(
    materialized='table',
    schema='raw_vpanchal'
) }}

WITH opportunities AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'opportunities') }}
)

SELECT
    *
FROM 
    opportunities