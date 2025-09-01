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
        account_owner_id
    FROM 
        {{ ref('accounts_view') }}
),

users AS (
    SELECT 
        userid
    FROM 
        {{ ref('users_view') }}
),

leads AS (
    SELECT
        leadid,
        lead_owner_id,
        lead_created_at,
        lead_status
    FROM 
        {{ ref('leads_view') }}
),

conversion_time AS (
    SELECT
        customerid,
        lead_created_at,
        subscription_start_date,
        subscription_start_date - lead_created_at AS days_to_convert
    FROM customers
    LEFT JOIN accounts
        ON customers.customer_account_id = accounts.accountid
    LEFT JOIN users
        ON accounts.account_owner_id = users.userid
    LEFT JOIN leads
        ON users.userid = leads.lead_owner_id
    LEFT JOIN subscriptions
        ON customerid = subscription_customer_id
    WHERE lead_status = 'Converted'
        AND subscription_start_date >= lead_created_at
)

SELECT * FROM conversion_time;

SELECT 
    CASE 
        WHEN days_to_convert <= 30 THEN '0-30'
        WHEN days_to_convert <= 60 THEN '31-60'
        WHEN days_to_convert <= 90 THEN '61-90'
        WHEN days_to_convert <= 120 THEN '91-120'
        WHEN days_to_convert <= 150 THEN '121-150'
        ELSE '151+'
    END as days_range,
    COUNT(DISTINCT customerid) as customers
FROM conversion_time
GROUP BY 
    CASE 
        WHEN days_to_convert <= 30 THEN '0-30'
        WHEN days_to_convert <= 60 THEN '31-60'
        WHEN days_to_convert <= 90 THEN '61-90'
        WHEN days_to_convert <= 120 THEN '91-120'
        WHEN days_to_convert <= 150 THEN '121-150'
        ELSE '151+'
    END