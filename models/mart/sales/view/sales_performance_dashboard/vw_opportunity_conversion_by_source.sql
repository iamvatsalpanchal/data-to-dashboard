{{ config(
    materialized='view'
) }}

with opportunities_view AS (
    SELECT
        *
    FROM 
        {{ ref('opportunities_view') }}
),

accounts_view AS (
    SELECT
        *
    FROM 
        {{ ref('accounts_view') }}
),

vw_opportunity_conversion_by_source AS (
	SELECT
	    a.account_industry AS industry,
	    COUNT(o.opportunityid) AS total_opportunities,
		COUNT(CASE WHEN opportunity_stage = 'Closed Won' THEN 1 END) AS closed_won,
        ROUND((COUNT(CASE WHEN opportunity_stage = 'Closed Won' THEN 1 END)::DECIMAL / COUNT(*) * 100), 2) AS conversion_rate
	FROM opportunities_view o
	JOIN accounts_view a ON o.opportunity_account_id = a.accountid
	GROUP BY a.account_industry
	order by closed_won desc
)

select * from vw_opportunity_conversion_by_source

