{%- set query_result_opp = run_query(var('query_opportunity_stage')) %}
{%- if execute -%}
{%- set opp_stage = query_result_opp.columns[0].values() -%}
{%- else -%}
{%- set opp_stage = [] -%}
{%- endif -%}

{%- set query_result_cases = run_query(var('query_case_priority')) %}
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
),

primary_contact as (
    select 
        pca.account_id,
        c.*
    from {{ ref('int_primary_contact_account')}} as pca
    inner join {{ ref('stg_salesforce__contact')}} as c
        on pca.contact_id = c.contact_id
),

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
        pc.salutation,
        pc.full_name,
        pc.title,
        pc.department,
        pc.mobilephone,
        pc.email,
        {% for stage in opp_stage -%}
        o.nr_opportunities_{{ stage }},
        {% endfor -%}
        {% for priority in case_priority -%}
        c.nr_cases_{{ priority }},
        {% endfor -%}
    from real_accounts as a
    left join user as u_owner
        on a.user_id_owner = u_owner.user_id
    left join cases_per_account as c
        on a.account_id = c.account_id
    left join opportunities_per_account as o
        on a.account_id = o.account_id
    left join primary_contact as pc
        on pc.account_id = pc.account_id
)

select * from combined_data