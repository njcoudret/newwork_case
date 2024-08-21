with leads as (
    select * from {{ ref('leads') }}
),

user as (
    select * from {{ ref('user') }}
),

combined_data as (
    select
        l.lead_id,
        l.company,
        l.state,
        l.country,
        l.source,
        l.status,
        l.status_type,
        l.is_unread_by_owner,
        u_owner.full_name as full_name_owner,
        u_create.full_name as full_name_create,
        u_last_modified.full_name as full_name_last_modified,
        l.is_converted,
        l.current_generators,
        l.product_series,
        1 as nr_leads,
        l.nr_locations
    from leads as l
    left join user as u_owner
        on l.user_id_owner = u_owner.user_id
    left join user as u_create
        on l.user_id_create = u_create.user_id
    left join user as u_last_modified
        on l.user_id_last_modified = u_last_modified.user_id
)

select * from combined_data