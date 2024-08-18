

with source as (

    select * from {{ source('salesforce', 'opportunity_history') }}

),

renamed as (

    select
        id as opportunity_history_id,
        opportunityid as opportunity_id,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('createddate')}} as datetime_create,
        {{to_local_tz('createddateforinsert')}} as datetime_create_for_insert,
        stagename as stage_name,
        amount,
        expectedrevenue as revenue_expected,
        {{c_date('closedate')}} as date_close,
        probability,
        fromforecastcategory as forecast_category_from,
        forecastcategory as forecast_category,
        prevforecastupdate as forecast_update_previous,
        fromopportunitystagename as stage_name_from,
        prevopportunitystageupdate as stage_update_previous,
        {{c_date('validthroughdate')}} as date_valid_through,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        {{bool('isdeleted')}} as is_deleted,
        prevamount as amt_previous,
        {{c_date('prevclosedate')}} as date_close_previous

    from source

)

select * from renamed
