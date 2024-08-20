{%- set query_result_opp = run_query('select distinct lower(stage_name) from staging.stg_salesforce__opportunity order by stage_sort_order asc') %}
{%- if execute -%}
{%- set opp_stage = query_result_opp.columns[0].values() -%}
{%- else -%}
{%- set opp_stage = [] -%}
{%- endif -%}

{%- set query_result_cases = run_query('select distinct lower(priority) from staging.stg_salesforce__case order by priority_order asc') %}
{%- if execute -%}
{%- set case_priority = query_result_cases.columns[0].values() -%}
{%- else -%}
{%- set case_priority = [] -%}
{%- endif -%}

with real_accounts as (
    select * from {{ ref('stg_salesforce__account')}}
    where 
        is_real_account
        and not is_deleted
),

accounts_with_countries as (
    select * from {{ ref('int_accounts_with_countries')}}
),

cases_per_account as (
    select * from {{ ref('int_cases_per_account')}}
),

opportunities_per_account as (
    select * from {{ ref('int_opportunities_aggregated_per_account')}}
),

user as (
    select * from {{ ref('stg_salesforce__user')}}
)

combined_data as (
    select
        a.account_id,
        a.name,
        a.type,
        a.state_billing,
        a.country_billing,
        a.industry,
        a.nr_employees,
        a.nr_locations,
        a.revenue_annual,
        a.ownership,
        u_owner.full_name as full_name_owner,
        a.is_active,
        a.rating,
        a.sla_category,
        a.customer_priority,
        {% for stage in opp_stage -%}
        o.nr_opportunities_{{ stage }},
        {% endfor -%}
        {% for priority in case_priority -%}
        o.nr_cases_{{ priority }},
        {% endfor -%}
    from real_accounts as a
    left join user as u_owner
        on a.user_id_owner = u_owner_user_id
    left join cases_per_account as c
        on a.account_id = c.account_id
    left join opportunities_per_account as o
        on a.account_id = o.account_id
)