WITH leads AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'leads') }}
)

SELECT
    *
FROM 
    leads