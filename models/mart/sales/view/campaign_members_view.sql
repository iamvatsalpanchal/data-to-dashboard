WITH campaign_members_view AS (
    SELECT
        campaignmemberid,
        campaignid AS campaign_member_campaign_id,
        leadid AS campaign_member_lead_id,
        status AS campaign_member_status
    FROM 
        {{ ref('dim_campaign_members') }}
)

SELECT
    *
FROM
    campaign_members_view
