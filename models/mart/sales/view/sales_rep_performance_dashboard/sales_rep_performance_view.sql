-- models/marts/sales/sales_rep_performance.sql
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
        lead_source
    FROM {{ ref('dim_leads') }}
),

accounts AS (
    SELECT
        account_owner_id,
        account_industry,
        account_created_at,
        account_annual_revenue,
        account_billing_country,
        account_customer_type
    FROM {{ ref('dim_accounts') }}
),

sales_rep_performance_view AS (
    SELECT
        userid,
        rep_name,
        COUNT(DISTINCT opportunityid) AS opportunities_owned,
        COUNT(DISTINCT CASE WHEN opportunity_stage = 'Closed Won' THEN opportunityid END) AS opportunities_closed,
        DIV(COUNT(DISTINCT CASE WHEN opportunity_stage = 'Closed Won' THEN opportunityid END) * 100, COUNT(DISTINCT opportunityid)) AS win_rate,
        ROUND(AVG(opportunity_close_date - opportunity_created_at), 2) AS avg_deal_cycle_days,
        COUNT(DISTINCT leadid) AS leads_gathered,
        SUM(opportunity_amount) AS total_revenue_generated,
        SUM(account_annual_revenue) AS total_account_revenue
    FROM users
    LEFT JOIN leads 
        ON users.userid = leads.lead_owner_id
    LEFT JOIN opportunities 
        ON users.userid = opportunities.opportunity_owner_id
    LEFT JOIN accounts
        ON users.userid = accounts.account_owner_id
    GROUP BY userid, rep_name
)

SELECT * FROM sales_rep_performance_view



