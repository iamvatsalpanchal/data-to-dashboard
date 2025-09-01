WITH accounts AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'accounts') }}
)

SELECT
    *
FROM 
    accounts