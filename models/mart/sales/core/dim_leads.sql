WITH dim_leads AS (
    SELECT
        leadid,
        CONCAT(firstname, ' ',lastname) AS lead_name,
        email AS lead_email,
        company AS lead_company,
        status AS lead_status,
        ownerid AS lead_owner_id,
        CAST(createddate AS DATE) AS lead_created_at,
        source AS lead_source,
        CAST(lastmodifieddate AS DATE) AS lead_last_modified_at
    FROM 
        {{ ref('leads') }}
)

SELECT
    *
FROM 
    dim_leads