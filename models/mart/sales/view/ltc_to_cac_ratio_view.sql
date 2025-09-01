WITH customers AS (
    SELECT
        *
    FROM 
        {{ ref('customers_view') }}
),

subscriptions AS (
    SELECT 
        *
    FROM 
        {{ ref('subscriptions_view') }}
),

accounts AS (
    SELECT
        *
    FROM 
        {{ ref('accounts_view') }}
),

users AS (
    SELECT 
        *
    FROM 
        {{ ref('users_view') }}
),

leads AS (
    SELECT
        *
    FROM 
        {{ ref('leads_view') }}
),

invoices AS (
    SELECT
        invoiceid,
        invoice_account_id,
        invoice_subscription_id,
        invoice_date,
        invoice_total_amount_due,
        invoice_paid
    FROM 
        {{ ref('invoices_view') }}
),

customer_ltv_cac AS (
    SELECT
        *
        -- EXTRACT(MONTH FROM customer_created_at) AS customer_month,
    FROM customers
    LEFT JOIN accounts
        ON customers.customer_account_id = accounts.accountid
    LEFT JOIN subscriptions
        ON customers.customerid = subscriptions.subscription_customer_id
    LEFT JOIN invoices
        ON subscriptions.subscriptionid = invoices.invoice_subscription_id
)

SELECT * FROM customer_ltv_cac