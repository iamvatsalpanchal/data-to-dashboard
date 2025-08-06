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

oppotunity_pipeline_by_stage_view AS (
    SELECT 
        opportunity_stage,
        COUNT(*) AS opportunity_cnt,
        ROUND(AVG(CASE WHEN opportunity_amount IS NOT NULL THEN CAST(opportunity_amount AS DECIMAL) END), 2) AS avg_deal_size,
        ROUND(AVG(CASE WHEN opportunity_probability IS NOT NULL THEN CAST(opportunity_probability AS DECIMAL) END), 2) AS avg_probability
    FROM 
        opportunities
    GROUP BY 
        opportunity_stage
)

SELECT * FROM oppotunity_pipeline_by_stage_view