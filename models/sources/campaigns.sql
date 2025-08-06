{{ config(
    materialized='table',
    schema='raw_vpanchal'
) }}

WITH campaigns AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'campaigns') }}
)

SELECT
    *
FROM 
    campaigns