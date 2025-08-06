WITH fct_subscriptions AS (
    SELECT
        *
    FROM 
        {{ ref('subscriptions') }}
)

SELECT
    *
FROM 
    fct_subscriptions