WITH subscriptions AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'subscriptions') }}
)

SELECT
    *
FROM 
    subscriptions