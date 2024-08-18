{%- set query_result = run_query('select distinct lower(type) from staging.stg_salesforce__case') %}

{%- if execute -%}
{%- set case_types = query_result.columns[0].values() -%}
{%- else -%}
{%- set case_types = [] -%}
{%- endif -%}

with products as (

    select * from {{ ref('stg_salesforce__product_2') }}

),

cases as (

    select * from {{ ref('stg_salesforce__case') }}
)

select
    p.product_id,
    {% for case_type in case_types %}
    count(case when lower(c.type) = '{{ case_type }}' then 1 end) as nr_cases_{{ case_type }},
    {% endfor %}    
    count(distinct c.case_id) as nr_cases_total
from {{ ref('stg_salesforce__case')}} as c
left join {{ ref('stg_salesforce__product_2')}} as p
    ON c.product_code = p.product_code
group by
    p.product_id