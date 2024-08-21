with relevant_cases as (
    select * from {{ ref('int_cases_joinable')}}
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
        c.user_id_owner,
        c.user_id_create,
        c.user_id_last_modified,
        1 as nr_cases
    from relevant_cases as c
)

select * from selected_columns