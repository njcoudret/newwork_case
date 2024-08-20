with source as (

    select * from {{ source('salesforce', 'contact') }}

),

renamed as (

    select
        id as contact_id,
        {{bool('isdeleted')}} as is_deleted,
        {{set_null('masterrecordid')}} as master_record_id,
        {{set_null('accountid')}} as account_id,
        salutation,
        firstname,
        lastname,
        otherstreet as street_other,
        othercity as city_other,
        otherstate as state_other,
        otherpostalcode as postalcode_other,
        othercountry as country_other,
        otherlatitude as latitude_other,
        otherlongitude as longitude_other,
        othergeocodeaccuracy as geocodeaccuracy_other,
        mailingstreet as street_mailing,
        mailingcity as city_mailing,
        mailingstate as state_mailing,
        mailingpostalcode as postalcode_mailing,
        mailingcountry as country_mailing,
        mailinglatitude as latitude_mailing,
        mailinglongitude as longitude_mailing,
        mailinggeocodeaccuracy as geocodeaccuracy_mailing,
        phone,
        fax,
        mobilephone,
        homephone,
        otherphone,
        assistantphone,
        reportstoid,
        email,
        title,
        department,
        assistantname,
        leadsource,
        birthdate,
        description,
        {{set_null('ownerid')}} as user_id_owner,
        {{bool('hasoptedoutofemail')}} as is_opt_out_email,
        {{bool('hasoptedoutoffax')}} as is_opt_out_fax,
        {{bool('donotcall')}} as is_opt_out_phone,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        {{to_local_tz('lastactivitydate')}} as datetime_last_activity,
        lastcurequestdate,
        lastcuupdatedate,
        emailbouncedreason,
        emailbounceddate,
        jigsaw,
        jigsawcontactid,
        cleanstatus,
        individualid,
        pronouns,
        genderidentity,
        level__c,
        languages__c

    from source

)

select * from renamed