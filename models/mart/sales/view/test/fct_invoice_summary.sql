WITH accounts AS (
    SELECT
        accountid,
        account_name,
        account_industry,
        account_owner_id
    FROM 
        {{ ref('accounts_view') }}
),

customers AS (
    SELECT
        customerid,
        customer_account_id,
        customer_created_at,
        customer_country
    FROM 
        {{ ref('customers_view') }}
),

subscriptions AS (
    SELECT
        subscriptionid,
        subscription_account_id,
        subscription_customer_id,
        subscription_start_date,
        subscription_end_date,
        subscription_status,
        subscription_plan
    FROM 
        {{ ref('subscriptions_view') }}
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

fct_invoice_summary AS (
    SELECT
        a.accountid,
        a.account_name,
        a.account_industry,
        i.invoice_date,
        i.invoice_total_amount_due,
        i.invoice_paid,
        c.customer_created_at,
        c.customer_country,
        s.subscription_start_date AS subscription_start_date,
        s.subscription_end_date AS subscription_end_date,
        s.subscription_status,
        s.subscription_plan
    FROM
        accounts a
    LEFT JOIN 
        customers c 
        ON a.accountid = c.customer_account_id
    LEFT JOIN
        invoices i
        ON a.accountid = i.invoice_account_id
        -- AND s.subscriptionid = i.invoice_subscription_id
    LEFT JOIN 
        subscriptions s 
        ON i.invoice_subscription_id = s.subscriptionid
)

SELECT 
    *
FROM
    fct_invoice_summary
WHERE accountid = 'A0100'

