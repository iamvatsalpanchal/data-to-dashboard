{{ config(
    materialized='view'
) }}

with opportunities_view AS (
    SELECT
        *
    FROM 
        {{ ref('opportunities_view') }}
),

vw_closed_won_revenue_trend AS (
	SELECT
	    EXTRACT(MONTH FROM opportunity_close_date) AS opportunity_closed_month,
		sum(opportunity_amount) AS closed_won_revenue
	FROM opportunities_view
	WHERE opportunity_stage = 'Closed Won'
	GROUP BY 
		EXTRACT(MONTH FROM opportunity_close_date)
	ORDER BY closed_won_revenue DESC
)

select * from vw_closed_won_revenue_trend