{%- set query_result = run_query(var('query_case_type')) %}
{%- if execute -%}
{%- set case_types = query_result.columns[0].values() -%}
{%- else -%}
{%- set case_types = [] -%}
{%- endif -%}

with products as (
    select * from {{ ref('stg_salesforce__product_2')}}
),

cases_per_product as (
    select * from {{ ref('int_cases_per_product')}}
),

products_with_cases as (
    select
        p.product_id,
        p.name,
        p.product_code,
        p.product_category,
        p.product_series,
        p.is_active,
        {% for case_type in case_types -%}
        c.nr_cases_{{ case_type }},
        {% endfor -%}
    from products as p
    left join cases_per_product as c
        on p.product_id = c.product_id
)

select * from products_with_cases