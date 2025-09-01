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