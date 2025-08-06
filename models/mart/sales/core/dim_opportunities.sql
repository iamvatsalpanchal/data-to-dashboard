WITH dim_opportunities AS (
    SELECT
        *
    FROM 
        {{ ref('opportunities') }}
)

SELECT
    *
FROM 
    dim_opportunities