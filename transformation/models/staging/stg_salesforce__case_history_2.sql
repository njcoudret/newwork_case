

with source as (

    select * from {{ source('salesforce', 'case_history_2') }}

),

renamed as (

    select
        id as case_history_id,
        caseid as case_id,
        {{set_null('ownerid')}} as user_id_owner,
        status,
        {{to_local_tz('previousupdate')}} as datetime_previous_update,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{bool('isdeleted')}} as is_deleted,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,

    from source

)

select * from renamed
