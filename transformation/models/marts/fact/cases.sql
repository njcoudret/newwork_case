with relevant_cases as (
    select * from {{ ref('int_cases_joinable')}}
),

users as (
    select * from {{ ref('stg_salesforce__user')}}
),

/* Cannot use status durations because of incorrect create dates */
-- status_durations as (
--     select * from {{ ref('int_case_status_durations')}}
-- ),

selected_columns as (
    select
        c.case_id,
        c.contact_id,
        c.account_id,
        c.product_id,
        c.case_number,
        c.type,
        c.status,
        c.is_closed,
        c.is_closed_on_create,
        c.reason,
        c.origin,
        c.subject,
        c.priority,
        c.priority_order,
        c.is_potential_liability,
        c.datetime_closed,
        u_owner.full_name as full_name_owner,
        1 as nr_cases
    from relevant_cases as c
    left join users as u_owner
        on c.user_id_owner = u_owner.user_id
)

select * from selected_columns