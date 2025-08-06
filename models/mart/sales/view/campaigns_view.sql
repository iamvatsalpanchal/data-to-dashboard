WITH campaigns_view AS (
    SELECT
        campaignid,
        name AS campaign_name,
        type AS campaign_type,
        startdate AS campaign_start_date,
        enddate AS campaign_end_date,
        budget AS campaign_budgeted_cost,
        expectedrevenue AS campaign_expected_revenue,
        ownerid AS campaign_owner_id
    FROM 
        {{ ref('dim_campaigns') }}
)

SELECT
    *
FROM
    campaigns_view