WITH dim_customers AS (
    SELECT
        *
    FROM 
        {{ ref('customers') }}
)

SELECT
    *
FROM 
    dim_customers