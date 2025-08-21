{{ config(
    materialized='view'
) }}

WITH leads AS (
    SELECT
        leadid,
        lead_status,
        CAST(lead_created_at AS TIMESTAMP) AS lead_created_at
    FROM 
        {{ ref('leads_view') }}
),

monthly_lead_generation_view AS (
    SELECT 
        DATE_TRUNC('month', lead_created_at) AS month,
        COUNT(*) AS leads_generated,
        COUNT(CASE WHEN lead_status = 'Converted' THEN 1 END) AS leads_converted,
        ROUND((COUNT(CASE WHEN lead_status = 'Converted' THEN 1 END)::DECIMAL / COUNT(*) * 100), 2) AS monthly_conversion_rate
    FROM leads
    GROUP BY 
        DATE_TRUNC('month', lead_created_at)
    ORDER BY monthly_conversion_rate DESC
)

SELECT 
    *
FROM
    monthly_lead_generation_view