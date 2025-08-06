WITH dim_users AS (
    SELECT
        *
    FROM 
        {{ ref('users') }}
)

SELECT
    *
FROM 
    dim_users