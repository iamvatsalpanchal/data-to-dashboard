WITH campaign_members AS (
    SELECT
        *
    FROM 
        {{ source('raw_vpanchal', 'campaign_members') }}
)

SELECT
    *
FROM 
    campaign_members