WITH leads_view AS (
    SELECT
        leadid,
        firstname AS lead_first_name,
        lastname AS lead_last_name,
        email AS lead_email,
        company AS lead_company,
        status AS lead_status,
        ownerid AS lead_owner_id,
        createddate AS lead_created_at,
        source AS lead_source
    FROM 
        {{ ref('dim_leads') }}
)

SELECT
    *
FROM
    leads_view