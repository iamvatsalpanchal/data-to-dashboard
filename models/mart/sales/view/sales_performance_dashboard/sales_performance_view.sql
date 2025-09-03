WITH opportunities AS (
    SELECT
        opportunityid,
        opportunity_account_id,
        opportunity_owner_id,
        opportunity_stage,
        opportunity_amount,
        opportunity_close_date,
        opportunity_created_at
    FROM {{ ref('dim_opportunities') }}
),

users AS (
    SELECT
        userid,
        user_name AS rep_name,
        user_region
    FROM {{ ref('dim_users') }}
),

leads AS (
    SELECT
        leadid,
        lead_name,
        lead_owner_id,
        lead_created_at,
        lead_status,
        lead_source
    FROM {{ ref('dim_leads') }}
),

accounts AS (
    SELECT
        accountid,
        account_owner_id,
        account_industry,
        account_created_at,
        account_annual_revenue,
        account_billing_country,
        account_customer_type
    FROM {{ ref('dim_accounts') }}
),

sales_performance_view AS (
    SELECT
        rep_name,
        EXTRACT(MONTH FROM opportunity_created_at) AS opportunity_creation_month,
        opportunity_stage,
        lead_status,
        lead_source, 
        COUNT(DISTINCT opportunityid) AS opportunities_in_stage,
        DIV(COUNT(DISTINCT CASE WHEN opportunity_stage = 'Closed Won' THEN opportunityid END) * 100, COUNT(DISTINCT opportunityid)) AS win_rate,
        SUM(opportunity_amount) FILTER(WHERE opportunity_stage = 'Closed Won') AS closed_won_revenue,
        AVG(opportunity_amount) AS avg_deal_size,
        COUNT(leadid) FILTER(WHERE lead_status = 'Converted') AS lead_to_opportunity_converted,
        DIV(COUNT(leadid) FILTER(WHERE lead_status = 'Converted') * 100, COUNT(DISTINCT leadid)) AS lead_to_opportunity_conversion_rate,
        COUNT(DISTINCT leadid) AS total_leads,
        AVG(lead_created_at - opportunity_created_at) FILTER(WHERE lead_status = 'Converted') AS avg_lead_to_opportunity_days,
        SUM(account_annual_revenue) AS ARR,
        DIV(account_annual_revenue, 12) AS MRR
    FROM opportunities
    LEFT JOIN users
        ON opportunities.opportunity_owner_id = users.userid
    LEFT JOIN leads 
        ON leads.lead_owner_id = users.userid
    LEFT JOIN accounts
        ON accounts.account_owner_id = users.userid
        AND accounts.accountid = opportunities.opportunity_account_id
    GROUP BY 
        EXTRACT(MONTH FROM opportunity_created_at), 
        opportunity_stage, 
        rep_name,
        lead_source,
        lead_status,
        account_annual_revenue
)

SELECT * FROM sales_performance_view