WITH leads AS (
    SELECT
        leadid,
        lead_owner_id,
        lead_status,
        lead_created_at,
        lead_source
    FROM {{ ref('dim_leads') }}
),

opportunities AS (
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

lead_generation_conversion_view AS (
    SELECT
        EXTRACT(MONTH FROM lead_created_at) AS lead_creation_month,
        lead_source,
        lead_status,
        COUNT(DISTINCT leadid) AS total_leads,
        ROUND(AVG(lead_created_at - opportunity_created_at),2)AS lead_to_opportunity_days
    FROM users
    LEFT JOIN leads 
        ON users.userid = leads.lead_owner_id
    LEFT JOIN opportunities 
        ON users.userid = opportunities.opportunity_owner_id
    WHERE lead_status = 'Converted'
    GROUP BY EXTRACT(MONTH FROM lead_created_at), lead_source, lead_status
)

SELECT * FROM lead_generation_conversion_view