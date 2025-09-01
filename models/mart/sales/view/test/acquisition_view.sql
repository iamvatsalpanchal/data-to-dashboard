WITH cohort_analysis_view AS (
    SELECT
        EXTRACT(MONTH FROM customer_created_at::DATE) AS acquisition_month,
        COUNT(DISTINCT customerid) AS customers_in_cohort
    FROM {{ref('customers_view') }} AS customers_view
    GROUP BY EXTRACT(MONTH FROM customer_created_at::DATE)
) 

SELECT
    *  
FROM 
    cohort_analysis_view