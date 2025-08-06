WITH dim_campaign_members AS (
    SELECT
        *
    FROM 
        {{ ref('campaign_members') }}
)

SELECT
    *
FROM 
    dim_campaign_members