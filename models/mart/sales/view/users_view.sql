WITH users_view AS (
    SELECT
        userid,
        name AS user_name,
        email as user_email,
        role AS user_role,
        region AS user_region,
        hiredate AS user_hired_at,
        active AS user_active
    FROM 
        {{ ref('dim_users') }}
)

SELECT
    *
FROM 
    users_view