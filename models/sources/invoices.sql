{{ config(
    materialized='table',
    schema='raw_vpanchal'
) }}

WITH invoices AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'invoices') }}
)

SELECT
    *
FROM 
    invoices