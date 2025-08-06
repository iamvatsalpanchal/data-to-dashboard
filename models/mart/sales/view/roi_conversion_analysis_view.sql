WITH leads AS (
    SELECT
        leadid,
        lead_company,
        lead_status,
        lead_owner_id,
        CAST(lead_created_at AS TIMESTAMP) AS lead_created_at,
        lead_source
    FROM 
        {{ ref('leads_view') }}
),

campaigns AS (
    SELECT
        campaignid,
        campaign_name,
        campaign_type,
        campaign_budgeted_cost,
        campaign_expected_revenue,
        CAST(campaign_start_date AS TIMESTAMP) AS campaign_start_date,
        CAST(campaign_end_date AS TIMESTAMP) AS campaign_end_date,
        campaign_owner_id
    FROM 
        {{ ref('campaigns_view') }}
),

campaign_members AS (
    SELECT
        campaignmemberid,
        campaign_member_campaign_id,
        campaign_member_lead_id,
        campaign_member_status
    FROM 
        {{ ref('campaign_members_view') }}
),

roi_conversion_analysis_view AS (
    SELECT 
        campaigns.campaign_name,
        campaigns.campaign_type,
        count(campaign_members.campaign_member_lead_id) AS leads_generated,
        CAST(campaigns.campaign_budgeted_cost AS DECIMAL) AS campaign_budget,
        ROUND(CAST(campaigns.campaign_budgeted_cost AS DECIMAL) / COALESCE(COUNT(campaign_members.campaign_member_lead_id), 0), 2) AS cost_per_lead,
        COUNT(CASE WHEN lead_status = 'Converted' THEN 1 END) AS converted_leads,
        ROUND((COUNT(CASE WHEN lead_status = 'Converted' THEN 1 END)::DECIMAL / COALESCE(COUNT(campaign_members.campaign_member_lead_id), 0) * 100), 2) AS campaign_conversion_rate
    FROM 
        campaigns
    LEFT JOIN 
        campaign_members ON campaigns.campaignid = campaign_members.campaign_member_campaign_id
    LEFT JOIN 
        leads ON campaign_members.campaign_member_lead_id = leads.leadid
    GROUP BY 
        campaigns.campaign_name, 
        campaigns.campaign_type,
        CAST(campaigns.campaign_budgeted_cost AS DECIMAL)
)

SELECT 
    *,
    CASE 
        WHEN campaign_conversion_rate >= 50 THEN 'Excellent'
        WHEN campaign_conversion_rate >= 30 THEN 'High Performer'
        WHEN campaign_conversion_rate >= 20 THEN 'Strong'
        ELSE 'Needs Review'
    END AS performance_rating  
FROM
    roi_conversion_analysis_view
