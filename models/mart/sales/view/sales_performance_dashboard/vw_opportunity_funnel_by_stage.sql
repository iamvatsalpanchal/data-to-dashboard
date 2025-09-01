{{ config(
    materialized='view'
) }}

with opportunities_view AS (
    SELECT
        *
    FROM 
        {{ ref('opportunities_view') }}
),

vw_opportunity_funnel_by_stage AS (
    select opportunity_stage, count(*) as opp_cnt
    from opportunities_view
    group by opportunity_stage
    order by opp_cnt desc
)

select * from vw_opportunity_funnel_by_stage
