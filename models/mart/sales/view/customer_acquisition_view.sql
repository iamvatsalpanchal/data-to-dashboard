

WITH customers AS (
    SELECT
        customerid,
        customer_account_id,
        customer_created_at
    FROM 
        {{ ref('customers_view') }}
),

customer_acquisition_view AS (
    SELECT
        EXTRACT(MONTH FROM customer_created_at) AS acquisition_month,
        COUNT(DISTINCT customerid) AS total_customers
    FROM 
        customers
    GROUP BY 
        EXTRACT(MONTH FROM customer_created_at)
)

SELECT
    *  
FROM 
    customer_acquisition_view