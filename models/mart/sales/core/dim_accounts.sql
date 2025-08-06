WITH dim_accounts AS (
    SELECT
        *
    FROM 
        {{ ref('accounts') }}
)

SELECT
    *
FROM 
    dim_accounts