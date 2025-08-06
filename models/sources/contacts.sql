{{ config(
    materialized='table',
    schema='raw_vpanchal'
) }}

WITH contacts AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'contacts') }}
)

SELECT
    *
FROM 
    contacts