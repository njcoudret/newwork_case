{%- set query_result = run_query(var('query_case_priority')) %}

{%- if execute -%}
{%- set case_priority = query_result.columns[0].values() -%}
{%- else -%}
{%- set case_priority = [] -%}
{%- endif -%}

with cases as (
    select * from {{ ref('stg_salesforce__case') }}
)

select
    c.account_id,
    {% for priority in case_priority -%}
    count(distinct case when lower(c.priority) = '{{ priority }}' then c.case_id end) as nr_cases_{{ priority }},
    {% endfor -%}    
    count(distinct c.case_id) as nr_cases_total
from cases as c
group by
    c.account_id