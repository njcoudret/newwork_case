{%- set query_result = run_query(var('query_case_type')) %}
{%- if execute -%}
{%- set case_types = query_result.columns[0].values() -%}
{%- else -%}
{%- set case_types = [] -%}
{%- endif -%}

with cases_joinable as (
    select * from {{ ref('int_cases_joinable') }}
    where product_id is not null
),

cases_per_product as (
    select
        c.product_id,
        {% for case_type in case_types -%}
        count(distinct case when lower(c.type) = '{{ case_type }}' then c.case_id end) as nr_cases_{{ case_type }},
        {% endfor -%}    
        count(distinct c.case_id) as nr_cases_total
    from cases_joinable as c
    group by
        c.product_id
)

select * from cases_per_product