WITH dim_contacts AS (
    SELECT
        *
    FROM 
        {{ ref('contacts') }}
)

SELECT
    *
FROM 
    dim_contacts