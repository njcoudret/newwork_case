with products as (

    select * from {{ ref('stg_salesforce__product_2') }}

),

select 

