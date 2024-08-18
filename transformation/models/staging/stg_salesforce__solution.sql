

with source as (

    select * from {{ source('salesforce', 'solution') }}

),

renamed as (

    select
        id as solution_id,
        {{bool('isdeleted')}} as is_deleted,
        solutionnumber as number,
        solutionname,
        {{bool('ispublished')}} as is_published,
        {{bool('ispublishedinpublickb')}} as is_published_public,
        status,
        {{bool('isreviewed')}} as is_reviewed,
        solutionnote,
        {{set_null('caseid')}} as case_id,
        {{set_null('ownerid')}} as user_id_owner,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        timesused,
        {{bool('ishtml')}} is_html

    from source

)

select * from renamed
