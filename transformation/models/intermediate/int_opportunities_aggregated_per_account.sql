{%- set query_result = run_query('select distinct lower(stage_name) from staging.stg_salesforce__opportunity order by stage_sort_order asc') %}

{%- if execute -%}
{%- set opp_stage = query_result.columns[0].values() -%}
{%- else -%}
{%- set opp_stage = [] -%}
{%- endif -%}

with opportunities as (
    select * from {{ ref('stg_salesforce__opportunity')}}
)

aggregated_opportunities_per_account as (
    select
        o.account_id,
        {% for stage in opp_stage -%}
        count(distinct case when lower(o.stage_name) = '{{ stage }}' then o.opportunity_id end) as nr_opportunities_{{ stage }},
        {% endfor -%}
        {% for stage in opp_stage -%}
        sum(case when lower(o.stage_name) = '{{ stage }}' then o.amount end) as amt_opportunities_{{ stage }},
        {% endfor -%}
        count(distinct o.opportunity_id) as nr_of_opportunities_total,
        sum(o.revenue_expected) as amt_opportunities_expected
        sum(o.amount) as amt_opportunities_total,
        
    from opportunities as o
)

select * from aggregated_opportunities_per_account