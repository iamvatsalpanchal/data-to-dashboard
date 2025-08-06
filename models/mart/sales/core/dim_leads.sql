WITH dim_leads AS (
    SELECT
        *
    FROM 
        {{ ref('leads') }}
)

SELECT
    *
FROM 
    dim_leads