WITH campaigns AS (
    SELECT
        campaignid,
        campaign_name,
        campaign_budgeted_cost,
        campaign_expected_revenue,
        campaign_type,
        campaign_start_date,
        campaign_end_date,
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
        userid,
        user_name,
        user_role
    FROM 
        {{ ref('users_view') }}
),

user_campaigns_view AS (
    SELECT
        *
    FROM campaigns
    LEFT JOIN users 
    ON campaigns.campaign_owner_id = users.userid
) 

select * from user_campaigns_view

-- leads AS (
--     SELECT
--         leadid,
--         CONCAT(lead_first_name,' ', lead_last_name) AS lead_full_name,
--         lead_company,
--         lead_status,
--         lead_source,
--         lead_owner_id
--     FROM 
--         {{ ref('leads_view') }}
-- ),