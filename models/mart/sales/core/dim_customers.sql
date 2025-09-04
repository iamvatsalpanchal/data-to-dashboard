WITH dim_customers AS (
    SELECT
        customerid,
        accountid AS customer_account_id,
        customeremail AS customer_email,
        CAST(createddate AS DATE) AS customer_created_at,
        country AS customer_country,
        CAST(lastmodifieddate AS DATE) AS customer_last_modified_at
    FROM 
        {{ ref('customers') }}
)

SELECT
    *
FROM 
    dim_customers