WITH fct_subscriptions AS (
    SELECT
        subscriptionid,
        accountid AS subscription_account_id,
        customerid AS subscription_customer_id,
        CAST(startdate AS DATE) AS subscription_start_date,
        CAST(enddate AS DATE) AS subscription_end_date,
        status AS subscription_status,
        plan AS subscription_plan
    FROM 
        {{ ref('subscriptions') }}
)

SELECT
    *
FROM 
    fct_subscriptions