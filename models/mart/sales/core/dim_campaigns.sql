WITH dim_campaigns AS (
    SELECT
        campaignid,
        name AS campaign_name,
        type AS campaign_type,
        CAST(startdate AS TIMESTAMP) AS campaign_start_date,
        CAST(enddate AS TIMESTAMP) AS campaign_end_date,
        CAST(budget AS DECIMAL) AS campaign_budgeted_cost,
        CAST(expectedrevenue AS DECIMAL) AS campaign_expected_revenue,
        ownerid AS campaign_owner_id
    FROM 
        {{ ref('campaigns') }}
)

SELECT
    *
FROM 
    dim_campaigns