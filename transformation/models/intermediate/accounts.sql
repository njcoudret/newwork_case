with accounts as (

    select * from {{ ref('stg_salesforce__account') }}

),

fix_country_names as (
    select 1
)

select * from fix_country_names