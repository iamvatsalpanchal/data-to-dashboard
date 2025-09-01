{{ config(
    materialized='view'
) }}

with opportunities_view AS (
    SELECT
        *
    FROM 
        {{ ref('opportunities_view') }}
),

users_view AS (
    SELECT
        *
    FROM 
        {{ ref('users_view') }}
),

vw_avg_deal_size_by_region_rep AS (
	SELECT
	    u.user_region,
	    u.user_name AS rep_name,
	    ROUND(AVG(o.opportunity_amount), 2) AS avg_deal_size,
	    MIN(o.opportunity_amount) AS min_deal_size,
	    MAX(o.opportunity_amount) AS max_deal_size	   
	FROM users_view u
	LEFT JOIN opportunities_view o ON o.opportunity_owner_id = u.userid
	WHERE o.opportunity_stage = 'Closed Won'
	GROUP BY u.user_region, u.user_name
)

select * from vw_avg_deal_size_by_region_rep

