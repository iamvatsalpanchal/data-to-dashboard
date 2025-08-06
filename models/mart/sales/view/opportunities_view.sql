WITH opportunities_view AS (
    SELECT
        opportunityid,
        accountid AS opportunity_account_id,
        ownerid AS opportunity_owner_id,
        stage AS opportunity_stage,
        amount AS opportunity_amount,
        closedate AS opportunity_close_date,
        createddate AS opportunity_created_at,
        probability AS opportunity_probability
    FROM 
        {{ ref('dim_opportunities') }}
)

SELECT 
    *
FROM
    opportunities_view