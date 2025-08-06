WITH subscriptions_view AS (
    SELECT
        subscriptionid,
        accountid AS subscription_account_id,
        customerid AS subscription_customer_id,
        startdate AS subscription_start_date,
        enddate AS subscription_end_date,
        status AS subscription_status,
        plan AS subscription_plan
    FROM 
        {{ ref('fct_subscriptions') }}
)

SELECT
    *
FROM
    subscriptions_view