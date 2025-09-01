WITH campaigns AS (
    SELECT
        campaignid,
        campaign_name,
        campaign_budgeted_cost:: DECIMAL AS campaign_budgeted_cost,
        campaign_expected_revenue:: DECIMAL AS campaign_expected_revenue,
        campaign_type,
        CAST(campaign_start_date AS TIMESTAMP) AS campaign_start_date,
        CAST(campaign_end_date AS TIMESTAMP) AS campaign_end_date,
        campaign_owner_id
    FROM 
        {{ ref('campaigns_view') }}
),

campaign_members AS (
    SELECT
        campaign_member_campaign_id,
        campaign_member_lead_id,
        campaign_member_status
    FROM 
        {{ ref('campaign_members_view') }}
),

users AS (
    SELECT
        userid
    FROM 
        {{ ref('users_view') }}
),

leads AS (
    SELECT
        leadid,
        CONCAT(lead_first_name,' ', lead_last_name) AS lead_full_name,
        lead_company,
        lead_status,
        lead_source,
        lead_owner_id
    FROM 
        {{ ref('leads_view') }}
),

campaign_performance_analysis_view AS (
    SELECT
        campaign_name,
        campaign_type,
        campaign_start_date::DATE,
        campaign_end_date::DATE,
        campaign_budgeted_cost AS budget,
        campaign_expected_revenue AS expected_revenue,
        COUNT(campaign_member_lead_id) AS leads_generated,
        COUNT(CASE WHEN campaign_member_status = 'Responded' THEN 1 END) AS leads_responded,
        COUNT(CASE WHEN lead_status = 'Converted' THEN 1 END) AS leads_converted,
        ROUND((COUNT(CASE WHEN lead_status = 'Converted' THEN 1 END)::DECIMAL / COALESCE(COUNT(campaign_member_lead_id), 0) * 100), 2) AS campaign_conversion_rate,
        ROUND((COUNT(CASE WHEN campaign_member_status = 'Responded' THEN 1 END)::DECIMAL / COALESCE(COUNT(campaign_member_lead_id), 0) * 100), 2) AS campaign_response_rate        
    FROM 
        campaigns
    LEFT JOIN users
        ON campaigns.campaign_owner_id = users.userid
    LEFT JOIN campaign_members
        ON campaign_members.campaign_member_campaign_id = campaigns.campaignid
    LEFT JOIN leads
        ON campaign_members.campaign_member_lead_id = leads.leadid
    GROUP BY 
        campaign_name,
        campaign_type,
        campaign_start_date,
        campaign_end_date,
        campaign_budgeted_cost,
        campaign_expected_revenue
)

SELECT
    *
FROM
    campaign_performance_analysis_view