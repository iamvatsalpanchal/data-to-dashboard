WITH customers AS (
    SELECT
        customerid,
        customer_account_id,
        customer_created_at
    FROM 
        {{ ref('customers_view') }}
),

subscriptions AS (
    SELECT 
        subscriptionid,
        subscription_customer_id,
        subscription_start_date,
        subscription_status,
        subscription_plan

    FROM 
        {{ ref('subscriptions_view') }}
),

accounts AS (
    SELECT
        accountid,
        account_name,
        account_industry,
        account_created_at,
        account_annual_revenue,
        account_customer_type
    FROM 
        {{ ref('accounts_view') }}
),

customers_cnt AS (
    SELECT 
        EXTRACT(MONTH FROM customer_created_at) AS customer_month,
        COUNT(DISTINCT customerid) AS total_customers
    FROM customers
    GROUP BY 
        EXTRACT(MONTH FROM customer_created_at)
),

account_cnt AS (
    SELECT 
        EXTRACT(MONTH FROM account_created_at) AS account_month,
        COUNT(DISTINCT accountid) AS total_accounts,
        SUM(account_annual_revenue) AS total_revenue
    FROM accounts
    GROUP BY 
        EXTRACT(MONTH FROM account_created_at)
),

-- SELECT * FROM account_cnt

subscription_growth AS (
    SELECT
        TO_CHAR(subscription_start_date, 'Mon YYYY') AS formatted_date,
        EXTRACT(MONTH FROM subscription_start_date) AS month,
        COUNT(DISTINCT subscriptionid) AS new_subscriptions,
        COUNT(CASE WHEN subscription_status = 'Active' THEN 1 END) AS active_subscriptions,
        COUNT(CASE WHEN subscription_status = 'Canceled' THEN 1 END) AS cancelled_subscriptions
    FROM subscriptions
    GROUP BY 
        EXTRACT(MONTH FROM subscription_start_date),
        TO_CHAR(subscription_start_date, 'Mon YYYY')
    ORDER BY month DESC
),

customer_subscription_view AS (
    SELECT
        customerid,
        customer_created_at,
        account_name,
        account_industry,
        account_annual_revenue,
        account_customer_type,
        subscription_start_date,
        subscription_status,
        subscription_plan
    FROM customers
    LEFT JOIN accounts
        ON customers.customer_account_id = accounts.accountid
    LEFT JOIN subscriptions
        ON customers.customerid = subscriptions.subscription_customer_id
)

SELECT
    *  
FROM 
    customer_subscription_view