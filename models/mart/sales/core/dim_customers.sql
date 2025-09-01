WITH dim_customers AS (
    SELECT
        customerid,
        accountid AS customer_account_id,
        customeremail AS customer_email,
        CAST(createddate AS DATE) AS customer_created_at,
        country AS customer_country
    FROM 
        {{ ref('customers') }}
)

SELECT
    *
FROM 
    dim_customers