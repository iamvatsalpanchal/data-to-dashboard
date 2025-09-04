WITH dim_opportunities AS (
    SELECT
        opportunityid,
        accountid AS opportunity_account_id,
        ownerid AS opportunity_owner_id,
        stage AS opportunity_stage,
        CAST(amount AS DECIMAL) AS opportunity_amount,
        CAST(closedate AS DATE) AS opportunity_close_date,
        CAST(createddate AS DATE) AS opportunity_created_at,
        probability AS opportunity_probability,
        CAST(lastmodifieddate AS DATE) AS opportunity_last_modified_at,
        leadid AS opportunity_lead_id
    FROM 
        {{ ref('opportunities') }}
)

SELECT
    *
FROM 
    dim_opportunities