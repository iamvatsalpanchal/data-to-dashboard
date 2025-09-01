WITH campaigns AS (
    SELECT
        campaignid,
        campaign_name,
        campaign_budgeted_cost:: DECIMAL AS campaign_budgeted_cost,
        campaign_type,
        CAST(campaign_start_date AS TIMESTAMP) AS campaign_start_date,
        CAST(campaign_end_date AS TIMESTAMP) AS campaign_end_date,
        campaign_owner_id
    FROM 
        {{ ref('campaigns_view') }}
),

leads AS (
    SELECT
        CONCAT(lead_first_name,' ', lead_last_name) AS lead_full_name,
        lead_company,
        lead_status,
        lead_source,
        lead_owner_id
    FROM 
        {{ ref('leads_view') }}
),

campaign_summary_view AS (
    SELECT
        campaign_type,
        COUNT(DISTINCT campaignid) AS total_active_campaigns,
        ROUND(SUM(campaign_budgeted_cost), 2) AS total_campaign_budget
        
    FROM 
        campaigns
    GROUP BY 
        campaign_type
)

SELECT
    *
FROM
    campaign_summary_view