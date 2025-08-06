WITH invoices_view AS (
    SELECT
        invoiceid,
        accountid AS invoice_account_id,
        subscriptionid AS invoice_subscription_id,
        invoicedate AS invoice_date,
        amountdue AS invoice_total_amount_due,
        paid AS invoice_paid
    FROM 
        {{ ref('fct_invoices') }}
)

SELECT
    *
FROM
    invoices_view