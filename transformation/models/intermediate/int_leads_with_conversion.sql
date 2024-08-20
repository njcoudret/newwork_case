with leads as (

    select * from {{ ref('stg_salesforce__lead') }}

),

transformed as (
    select
        lead_id,
        if(
            status = 'Converted',
            true,
            false
        ) as is_converted
    from leads
)

select * from transformed