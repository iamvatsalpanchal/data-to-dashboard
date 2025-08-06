WITH accounts AS (
    SELECT
        accountid,
        account_name,
        account_industry,
        account_owner_id,
        account_created_at,
        account_annual_revenue,
        account_billing_city,
        account_billing_country,
        account_customer_type
    FROM 
        {{ ref('accounts_view') }}
),

opportunities AS (
    SELECT 
        opportunityid,
        opportunity_account_id,
        opportunity_owner_id,
        opportunity_stage,
        opportunity_amount,
        opportunity_created_at,
        opportunity_close_date,
        opportunity_probability
    FROM 
        {{ ref('opportunities_view') }}
),

users AS (
    SELECT 
        userid,
        user_name,
        user_role,
        user_active
    FROM 
        {{ ref('users_view') }}
),

customers AS (
    SELECT
        customerid,
        customer_account_id,
        customer_created_at,
        customer_country
    FROM 
        {{ ref('customers_view') }}
),

subscriptions AS (
    SELECT
        subscriptionid,
        subscription_account_id,
        subscription_customer_id,
        subscription_start_date,
        subscription_end_date,
        subscription_status,
        subscription_plan
    FROM 
        {{ ref('subscriptions_view') }}
),

invoices AS (
    SELECT
        invoiceid,
        invoice_account_id,
        invoice_subscription_id,
        invoice_date,
        invoice_total_amount_due,
        invoice_paid
    FROM 
        {{ ref('invoices_view') }}
),

contacts AS (
    SELECT
        contactid,
        contact_account_id,
        contact_first_name,
        contact_last_name,
        contact_email,
        contact_phone,
        contact_title,
        contact_city
    FROM 
        {{ ref('contacts_view') }}
),

leads AS (
    SELECT
        leadid,
        lead_first_name,
        lead_last_name,
        lead_email,
        lead_company,
        lead_status,
        lead_owner_id,
        lead_created_at,
        lead_source
    FROM 
        {{ ref('leads_view') }}
),

campaigns AS (
    SELECT
        campaignid,
        campaign_name,
        campaign_type,
        campaign_start_date,
        campaign_end_date,
        campaign_owner_id
    FROM 
        {{ ref('campaigns_view') }}
),

campaign_members AS (
    SELECT
        campaignmemberid,
        campaign_member_campaign_id,
        campaign_member_lead_id,
        campaign_member_status
    FROM 
        {{ ref('campaign_members_view') }}
),

account_360_view AS (
    SELECT 
        accountid,
        account_name,
        account_industry,
        account_created_at,
        account_customer_type,
        COUNT(DISTINCT contactid) AS contact_count
    FROM accounts
    LEFT JOIN
        contacts
        ON accounts.accountid = contacts.contact_account_id
    LEFT JOIN
        opportunities
        ON accounts.accountid = opportunities.opportunity_account_id
    LEFT JOIN
        subscriptions
        ON accounts.accountid = subscriptions.subscription_account_id
    LEFT JOIN
        invoices
        ON accounts.accountid = invoices.invoice_account_id
    LEFT JOIN
        users
        ON accounts.account_owner_id = users.userid
    LEFT JOIN
        campaigns
        ON users.userid = campaigns.campaign_owner_id
    LEFT JOIN
        leads
        ON leads.lead_owner_id = users.userid
    LEFT JOIN
        campaign_members
        ON campaign_members.campaign_member_campaign_id = campaigns.campaignid
        OR campaign_members.campaign_member_lead_id = leads.leadid
    GROUP BY
        accountid,
        account_name,
        account_industry,
        account_created_at,
        account_customer_type
) SELECT * FROM account_360_view WHERE accountid = 'A0100'