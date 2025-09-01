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