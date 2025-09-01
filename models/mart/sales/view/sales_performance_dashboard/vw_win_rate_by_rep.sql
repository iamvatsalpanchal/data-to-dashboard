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

vw_win_rate_by_rep AS (
	SELECT
	    u.userid,
	    u.user_name AS rep_name,
	    COUNT(o.opportunityid) AS total_opportunities,
	    COUNT(o.opportunityid) FILTER (WHERE o.opportunity_stage = 'Closed Won') AS closed_won,
	    ROUND(
	        100.0 * COUNT(o.opportunityid) FILTER (WHERE o.opportunity_stage = 'Closed Won') / COALESCE(COUNT(o.opportunityid),0),
	        2
	    ) AS win_rate_percentage
	FROM opportunities_view o
	JOIN users_view u ON o.opportunity_owner_id = u.userid
	GROUP BY u.userid, u.user_name
)

select * from vw_win_rate_by_rep