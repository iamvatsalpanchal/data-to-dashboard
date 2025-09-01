WITH dim_accounts AS (
    SELECT
        accountid,
        accountname AS account_name,
        industry AS account_industry,
        ownerid AS account_owner_id,
        CAST(createddate AS DATE) AS account_created_at,
        CAST(annualrevenue AS DECIMAL) AS account_annual_revenue,
        billingcity AS account_billing_city,
        billingcountry AS account_billing_country,
        customertype AS account_customer_type
    FROM 
        {{ ref('accounts') }}
)

SELECT
    *
FROM 
    dim_accounts