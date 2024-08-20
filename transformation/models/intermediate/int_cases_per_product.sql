{%- set query_result = run_query('select distinct lower(type) from staging.stg_salesforce__case') %}

{%- if execute -%}
{%- set case_types = query_result.columns[0].values() -%}
{%- else -%}
{%- set case_types = [] -%}
{%- endif -%}

with cases as (
    select * from {{ ref('stg_salesforce__case') }}
)

select
    c.product_id,
    {% for case_type in case_types -%}
    count(distinct case when lower(c.type) = '{{ case_type }}' then c.case_id end) as nr_cases_{{ case_type }},
    {% endfor -%}    
    count(distinct c.case_id) as nr_cases_total
from cases as c
group by
    c.product_id