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

customer_metrics AS (
    SELECT
        customerid,
        customer_created_at,
        subscription_start_date,
        EXTRACT(MONTH FROM subscription_start_date) AS customer_subscription_month,
        CASE 
            WHEN subscription_start_date = customer_created_at THEN 'New'
            WHEN subscription_start_date > customer_created_at THEN 'Returning'
            ELSE 'New'
        END AS customer_type

    FROM customers
    LEFT JOIN subscriptions
        ON customers.customerid = subscriptions.subscription_customer_id
)
SELECT 
    customer_subscription_month,
    COUNT(CASE WHEN customer_type = 'New' THEN 1 END) AS new_customers,
    COUNT(CASE WHEN customer_type = 'Returning' THEN 1 END) AS returning_customers  
FROM customer_metrics
GROUP BY 
    customer_subscription_month
ORDER BY customer_subscription_month