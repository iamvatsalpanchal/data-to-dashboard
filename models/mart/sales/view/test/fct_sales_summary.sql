WITH accounts AS (
    SELECT
        accountid,
        account_name,
        account_industry,
        account_owner_id
    FROM 
        {{ ref('accounts_view') }}
    WHERE accountid = 'A0102'
),

opportunities AS (
    SELECT 
        opportunity_account_id,
        opportunity_owner_id,
        opportunity_stage,
        opportunity_amount,
        opportunity_created_at,
        opportunity_close_date,
        opportunity_probability
    FROM 
        {{ ref('opportunities_view') }}
    WHERE opportunity_account_id = 'A0102'
),

users AS (
    SELECT 
        userid,
        user_name,
        user_role,
        user_active
    FROM 
        {{ ref('users_view') }}
    WHERE userid = 'U009'
),

fct_sales_summary AS (
    SELECT
        a.accountid,
        a.account_name,
        a.account_industry,
        o.opportunity_stage,
        o.opportunity_amount,
        o.opportunity_created_at,
        o.opportunity_close_date,
        o.opportunity_probability,
        u.user_name,
        u.user_role,
        u.user_active
    FROM 
        accounts a
    LEFT JOIN 
        opportunities o ON a.accountid = o.opportunity_account_id
    LEFT JOIN 
        users u ON o.opportunity_owner_id = u.userid
)

SELECT * FROM fct_sales_summary