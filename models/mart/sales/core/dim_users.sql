WITH dim_users AS (
    SELECT
        userid,
        name AS user_name,
        email as user_email,
        role AS user_role,
        region AS user_region,
        CAST(hiredate AS DATE) AS user_hired_at,
        active AS user_active,
        CAST(lastmodifieddate AS DATE) AS user_last_modified_at
    FROM 
        {{ ref('users') }}
)

SELECT
    *
FROM 
    dim_users