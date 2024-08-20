with relevant_cases as (
    select * from {{ ref('stg_salesforce__case')}}
    where not is_deleted
),

status_durations_raw as (
    select 
        case_id,
        date_part('day',coalesce(datetime_closed,current_date) - datetime_create) as days_open
    from relevant_cases
),

status_durations_working_days as (
    select
        case_id,
        days_open,
        days_open - ((days_open / 7)*2) as days_working_open
    from status_durations_raw
)

select * from status_durations_working_days