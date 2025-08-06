WITH fct_invoices AS (
    SELECT
        *
    FROM 
        {{ ref('invoices') }}
)

SELECT
    *
FROM 
    fct_invoices