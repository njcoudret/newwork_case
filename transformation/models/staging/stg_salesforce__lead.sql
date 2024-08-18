

with source as (

    select * from {{ source('salesforce', 'lead') }}

),

renamed as (

    select
        id as lead_id,
        {{bool('isdeleted')}} as is_deleted,
        {{set_null('masterrecordid')}} as master_record_id,
        salutation,
        firstname as first_name,
        lastname as last_name,
        title,
        company,
        street,
        city,
        state,
        postalcode,
        country,
        latitude,
        longitude,
        geocodeaccuracy,
        phone,
        mobilephone,
        fax,
        email,
        website,
        description,
        leadsource as source,
        CASE
            WHEN status LIKE 'Working%' THEN 'Working'
            WHEN status LIKE 'Open%' THEN 'Open'
            WHEN status LIKE 'Closed%' THEN 'Closed'
            ELSE 'Other'
        END AS status,
        CASE
            WHEN status LIKE '%- Contacted' THEN 'Contacted'
            WHEN status LIKE '%- Not Contacted' THEN 'Not Contacted'
            WHEN status LIKE '%- Converted' THEN 'Converted'
            WHEN status LIKE '%- Not Converted' THEN 'Not Converted'
            ELSE 'Other'
        END AS status_type,
        industry,
        rating,
        annualrevenue as revenue_annual,
        numberofemployees as nr_employees,
        {{set_null('ownerid')}} as user_id_owner,
        {{bool('hasoptedoutofemail')}} as is_opt_out_email,
        {{bool('isconverted')}} as is_converted,
        {{c_date('converteddate')}} as date_converted,
        {{set_null('convertedaccountid')}} as account_id_converted,
        {{set_null('convertedcontactid')}} as contact_id_converted,
        {{set_null('convertedopportunityid')}} as opportunity_id_converted,
        {{bool('isunreadbyowner')}} as is_unread_by_owner,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        {{to_local_tz('lastactivitydate')}} as datetime_last_activity,
        {{bool('donotcall')}} as is_opt_out_phone,
        {{bool('hasoptedoutoffax')}} as is_opt_out_fax,
        {{to_local_tz('lasttransferdate')}} as datetime_last_transfer,
        jigsaw,
        jigsawcontactid,
        cleanstatus,
        companydunsnumber,
        dandbcompanyid,
        emailbouncedreason,
        emailbounceddate,
        individualid,
        pronouns,
        genderidentity,
        siccode__c,
        replace(
            productinterest__c,' series',''
        ) as product_series,
        {{bool('primary__c')}} as is_primary,
        currentgenerators__c as current_generators,
        numberoflocations__c as nr_locations

    from source

)

select * from renamed
