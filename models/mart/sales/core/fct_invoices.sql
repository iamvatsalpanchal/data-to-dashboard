WITH fct_invoices AS (
    SELECT
        invoiceid,
        accountid AS invoice_account_id,
        subscriptionid AS invoice_subscription_id,
        CAST(invoicedate AS DATE) AS invoice_date,
        CAST(amountdue AS DECIMAL) AS invoice_total_amount_due,
        paid AS invoice_paid
    FROM 
        {{ ref('invoices') }}
)

SELECT
    *
FROM 
    fct_invoices