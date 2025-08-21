{{ config(
    materialized='view'
) }}

WITH leads AS (
    SELECT
        lead_status,
        lead_source
    FROM 
        {{ ref('leads_view') }}
),

lead_source_performance_view AS (
    SELECT 
        lead_source,
        COUNT(*) AS leads_count,
        COUNT(CASE WHEN lead_status = 'Converted' THEN 1 END) AS converted_count,
        ROUND((COUNT(CASE WHEN lead_status = 'Converted' THEN 1 END)::DECIMAL / COUNT(*) * 100), 2) AS conversion_rate
    FROM leads
    GROUP BY 
        lead_source
)

SELECT 
    *
FROM
    lead_source_performance_view