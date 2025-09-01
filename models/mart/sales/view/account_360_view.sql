{{ config(
    materialized='view'
) }}

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
        lead_name,
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
        -- Accounts
        accountid,
        account_name,
        account_industry,
        account_created_at,
        account_customer_type,

        -- Contacts
        COUNT(DISTINCT contactid) AS contact_count,

        -- Opportunities/Pipeline
        COUNT(DISTINCT opportunityid) AS total_opportunities,
        COUNT(DISTINCT opportunityid) FILTER (WHERE opportunity_stage = 'Closed Won') AS opportunities_won,
        SUM(CAST(opportunity_amount AS DECIMAL)) AS total_potential_revenue,
        AVG(CAST(opportunity_amount AS DECIMAL)) AS avg_deal_size,
        -- ROUND
        --     (
        --         COUNT(DISTINCT opportunityid) FILTER (WHERE opportunity_stage = 'Closed Won') * 100 /
        --         COALESCE(COUNT(DISTINCT opportunityid) FILTER (WHERE opportunity_stage IN ('Closed Won','Closed Lost', 'Proposal', 'Negotiation', 'Prospecting', 'Qualification')), 0), 2
        --     ) 
        -- AS win_rate,
        MAX(opportunity_close_date) AS last_opportunity_date,


        -- Invoice/Revenue
        COALESCE(SUM(CAST(invoice_total_amount_due AS DECIMAL)) FILTER (WHERE invoice_paid = 'true'), 0) AS total_revenue_paid,
        COALESCE(SUM(CAST(invoice_total_amount_due AS DECIMAL)) FILTER (WHERE invoice_paid= 'false'), 0) AS total_revenue_unpaid,
        MAX(invoice_date) AS last_invoice_date,


        -- Subscriptions
        COUNT(DISTINCT subscriptionid) AS total_subscriptions,
        COUNT(DISTINCT subscriptionid) FILTER (WHERE subscription_status = 'Active') AS active_subscriptions,
        MAX(subscription_end_date) FILTER (WHERE subscription_status = 'Active') AS next_renewal_date


        -- Campaigns/Leads
        -- COUNT(DISTINCT campaignid) AS campaigns_participated,
        -- COUNT(DISTINCT leadid) AS total_leads,   
        -- COUNT(DISTINCT lead_source) AS lead_sources

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
)

SELECT * FROM account_360_view 
