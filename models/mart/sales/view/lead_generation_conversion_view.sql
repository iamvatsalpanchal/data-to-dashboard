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

lead_genearation_conversion_view AS (
    SELECT 
        leadid,
        lead_company,
        COALESCE(COUNT(DISTINCT leads.leadid), 0) AS total_leads,
        COUNT(DISTINCT leads.leadid) FILTER (WHERE lead_status = 'Converted') AS converted_leads
    FROM leads
    GROUP BY 
        leadid,
        lead_company
    ORDER BY 
        total_leads DESC
)

SELECT * FROM lead_genearation_conversion_view 
