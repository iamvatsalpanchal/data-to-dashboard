-- models/marts/sales/sales_rep_performance.sql
WITH accounts AS (
    SELECT
        accountid,
        account_owner_id,
        account_industry,
        account_created_at,
        account_annual_revenue,
        account_billing_country,
        account_customer_type
    FROM {{ ref('dim_accounts') }}
),

subscriptions AS (
    SELECT
        subscriptionid,
        subscription_account_id,
        subscription_start_date,
        subscription_end_date,
        subscription_status,
        subscription_plan
    FROM {{ ref('fct_subscriptions') }}
),

invoices AS (
    SELECT
        invoiceid,
        invoice_account_id,
        invoice_date,
        invoice_total_amount_due AS invoice_amount,
        invoice_paid
    FROM {{ ref('fct_invoices') }}
),

revenue_subscription_analytics_view AS (
    SELECT
        accountid,
        subscription_plan,
        -- EXTRACT(MONTH FROM subscription_start_date) AS subscription_start_month,
        
        COUNT(DISTINCT subscriptionid) FILTER(WHERE subscription_status = 'Active') AS active_subscriptions,
        -- EXTRACT(MONTH FROM account_created_at) AS account_creation_month,
        -- DIV(COUNT(accountid) FILTER(WHERE account_customer_type = 'Churned') * 100, COUNT(accountid) FILTER(WHERE account_customer_type IN ('Existing', 'Churned'))) AS gross_churn_rate,
        -- DIV(COUNT(accountid) FILTER(WHERE account_customer_type = 'Churned') * 100, COUNT(accountid) FILTER(WHERE account_customer_type IN ('Existing', 'Churned', 'New'))) AS net_churn_rate,
        SUM(CASE WHEN invoice_paid = 'true' THEN invoice_amount ELSE 0 END) AS paid_invoice_amount,
        SUM(CASE WHEN invoice_paid = 'false' THEN invoice_amount ELSE 0 END) AS unpaid_invoice_amount,
        ROUND(AVG(subscription_end_date - subscription_start_date), 2) AS avg_subscription_days
    FROM accounts
    LEFT JOIN invoices 
        ON accounts.accountid = invoices.invoice_account_id
    LEFT JOIN subscriptions 
        ON accounts.accountid = subscriptions.subscription_account_id
    GROUP BY accountid, subscription_plan
    --  EXTRACT(MONTH FROM subscription_start_date)
)

SELECT * FROM revenue_subscription_analytics_view



