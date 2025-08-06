WITH customers_view AS (
    SELECT
        customerid,
        accountid AS customer_account_id,
        customeremail AS customer_email,
        createddate AS customer_created_at,
        country AS customer_country
    FROM 
        {{ ref('dim_customers') }}
)

SELECT
    *
FROM
    customers_view