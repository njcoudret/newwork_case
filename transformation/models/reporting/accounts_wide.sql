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

with account as (
    select * from {{ ref('account')}}
),

user as (
    select * from {{ ref('user')}}
),

date as (
    select * from {{ ref('date')}}
),

cases_per_account as (
    select
        c.account_id,
        {% for priority in case_priority -%}
        count(distinct case when lower(c.priority) = '{{ priority }}' then c.case_id end) as nr_cases_{{ priority }},
        {% endfor -%}    
        count(distinct c.case_id) as nr_cases_total
    from {{ ref('cases') }} as c
    group by
        c.account_id
),

contact as (
    select * from {{ ref('contact') }}
),

opportunities_per_account as (
    select
        o.account_id,
        {% for stage in opp_stage -%}
        count(distinct case when lower(o.stage_name_underscored) = '{{ stage }}' then o.opportunity_id end) as nr_opportunities_{{ stage }},
        {% endfor -%}
        {% for stage in opp_stage -%}
        sum(case when lower(o.stage_name_underscored) = '{{ stage }}' then o.amount end) as amt_opportunities_{{ stage }},
        {% endfor -%}
        count(distinct o.opportunity_id) as nr_of_opportunities_total,
        sum(o.revenue_expected) as amt_opportunities_expected,
        sum(o.amount) as amt_opportunities_total
    from {{ ref('opportunities') }} as o
    group by
        o.account_id
),

combined_data as (
    select
        a.account_id,
        a.datetime_create,
        d.date as date_create,
        d.year_nr as year_nr_create,
        d.month_nr as month_nr_create,
        d.iso_week_nr as iso_week_nr_create,
        a.name,
        a.type,
        a.state_billing,
        a.country_billing,
        a.state_shipping,
        a.country_shipping,
        a.industry,
        a.nr_employees,
        a.nr_locations,
        a.revenue_annual,
        a.ownership,
        u_owner.full_name as full_name_owner,
        u_create.full_name as full_name_create,
        u_last_modified.full_name as full_name_last_modified,
        a.is_active,
        a.rating,
        a.sla_category,
        a.customer_priority,
        pc.full_name as full_name_primary_contact,
        pc.department as department_primary_contact,
        pc.email as email_primary_contact,
        {% for stage in opp_stage -%}
        o.nr_opportunities_{{ stage }},
        {% endfor -%}
        {% for priority in case_priority -%}
        c.nr_cases_{{ priority }},
        {% endfor -%}
    from account as a
    left join cases_per_account as c
        on a.account_id = c.account_id
    left join opportunities_per_account as o
        on a.account_id = o.account_id
    left join date as d
        on {{c_date('a.datetime_create')}} = d.date
    left join user as u_owner
        on a.user_id_owner = u_owner.user_id
    left join user as u_create
        on a.user_id_create = u_create.user_id
    left join user as u_last_modified
        on a.user_id_last_modified = u_last_modified.user_id
    left join contact as pc
        on a.contact_id_primary = pc.contact_id
)

select * from combined_data