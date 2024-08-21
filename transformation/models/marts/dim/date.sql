with date as (
    select * from {{ ref('stg_seed__date')}}
)

select * from date