with users as (
    select * from {{ ref('stg_salesforce__user')}}
),

relevant_columns as (
    select
        user_id,
        user_name,
        first_name,
        last_name,
        full_name,
        alias,
        is_active
    from users
)

select * from relevant_columns