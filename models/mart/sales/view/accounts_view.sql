WITH accounts_view AS (
    SELECT
        accountid,
        accountname AS account_name,
        industry AS account_industry,
        ownerid AS account_owner_id,
        createddate AS account_created_at,
        annualrevenue AS account_annual_revenue,
        billingcity AS account_billing_city,
        billingcountry AS account_billing_country,
        customertype AS account_customer_type
    FROM 
        {{ ref('dim_accounts') }}
)

SELECT
    *
FROM 
    accounts_view