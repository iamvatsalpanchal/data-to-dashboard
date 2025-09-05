WITH leads AS (
    SELECT
        leadid,
        lead_owner_id,
        lead_status,
        lead_last_modified_at,
        lead_source,
        lead_created_at
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
        lead_last_modified_at,
        lead_source,
        lead_status,
        leadid,
        rep_name,
        user_region
    FROM leads
    LEFT JOIN users
        ON users.userid = leads.lead_owner_id
)

SELECT * FROM lead_generation_conversion_view