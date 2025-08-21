{{ config(
    materialized='view'
) }}

WITH opportunities AS (
    SELECT 
        opportunityid,
        opportunity_stage,
        opportunity_amount,
        opportunity_created_at,
        opportunity_close_date,
        opportunity_probability
    FROM 
        {{ ref('opportunities_view') }}
),

oppotunity_metrics_view AS (
    SELECT 
        COUNT(*) AS opportunity_cnt,
        COUNT(CASE WHEN opportunity_stage = 'Closed Won' THEN 1 END) AS won_opportunity,
        COUNT(CASE WHEN opportunity_stage = 'Closed Lost' THEN 1 END) AS loss_opportunity,
        SUM(CASE WHEN opportunity_stage = 'Closed Won' THEN CAST(opportunity_amount AS DECIMAL) END) AS won_revenue,

        ROUND(AVG(CASE WHEN opportunity_stage = 'Closed Won' AND opportunity_amount != '' AND opportunity_amount IS NOT NULL THEN CAST(opportunity_amount AS DECIMAL) END), 2) AS avg_deal_size,
        ROUND((COUNT(CASE WHEN opportunity_stage = 'Closed Won' THEN 1 END)::DECIMAL / COUNT(*) * 100), 2) AS opportunity_win_rate
    FROM 
        opportunities
)

SELECT * FROM oppotunity_metrics_view