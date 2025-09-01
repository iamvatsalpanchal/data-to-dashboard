WITH dim_campaign_members AS (
    SELECT
        campaignmemberid,
        campaignid AS campaign_member_campaign_id,
        leadid AS campaign_member_lead_id,
        status AS campaign_member_status
    FROM 
        {{ ref('campaign_members') }}
)

SELECT
    *
FROM 
    dim_campaign_members