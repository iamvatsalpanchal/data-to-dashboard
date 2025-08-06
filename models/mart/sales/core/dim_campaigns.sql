WITH dim_campaigns AS (
    SELECT
        *
    FROM 
        {{ ref('campaigns') }}
)

SELECT
    *
FROM 
    dim_campaigns