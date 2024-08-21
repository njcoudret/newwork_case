with product as (
    select * from {{ ref('stg_salesforce__product_2')}}
),

selected_columns as (
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
    from product as p
)

select * from selected_columns