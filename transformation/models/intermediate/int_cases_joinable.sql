with cases as (
    select * from {{ ref('stg_salesforce__case')}}
),

products as (
    select * from {{ ref('stg_salesforce__product_2')}}
),

cases_joinable as (
    select
        c.*,
        p.product_id
    from cases as c
    left join products as p
        on c.product_code = p.product_code
)

select * from cases_joinable