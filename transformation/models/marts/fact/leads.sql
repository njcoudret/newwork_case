with leads as (
    select * from {{ ref('stg_salesforce__lead') }}
    where not is_deleted
),

leads_converted as (
    select * from {{ ref('int_leads_with_conversion') }}
),

leads_with_conversion_and_owner as (
    select
        l.lead_id,
        l.company,
        l.state,
        l.country,
        l.source,
        l.status,
        l.status_type,
        l.is_unread_by_owner,
        l.user_id_owner,
        l.user_id_create,
        l.user_id_last_modified,
        lc.is_converted,
        l.current_generators,
        l.product_series,
        1 as nr_leads,
        l.nr_locations
    from leads as l
    left join leads_converted as lc
        on l.lead_id = lc.lead_id
)

select * from leads_with_conversion_and_owner