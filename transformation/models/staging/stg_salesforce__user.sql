
with source as (

    select * from {{ source('salesforce', 'user') }}

),

renamed as (

    select
        id as user_id,
        username as user_name,
        firstname as first_name,
        lastname as last_name,
        CONCAT_WS(
            ', ',
            IFNULL(lastname,''),
            IFNULL(firstname,'')
        ) as full_name,
        companyname as company_name,
        division,
        department,
        title,
        street,
        city,
        state,
        postalcode,
        country,
        latitude,
        longitude,
        geocodeaccuracy,
        email,
        senderemail,
        sendername,
        signature,
        stayintouchsubject,
        stayintouchsignature,
        stayintouchnote,
        phone,
        fax,
        mobilephone,
        alias,
        communitynickname,
        {{bool('isactive')}} as is_active,
        {{bool('issystemcontrolled')}} as is_system_controlled,
        timezonesidkey,
        userroleid,
        localesidkey,
        {{bool('receivesinfoemails')}} as is_receives_infomails,
        receivesadmininfoemails,
        emailencodingkey,
        profileid,
        usertype,
        usersubtype,
        startday,
        endday,
        languagelocalekey,
        employeenumber,
        delegatedapproverid,
        managerid,
        lastlogindate,
        lastpasswordchangedate,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        numberoffailedlogins,
        suaccessexpirationdate,
        suorgadminexpirationdate,
        offlinetrialexpirationdate,
        wirelesstrialexpirationdate,
        offlinepdatrialexpirationdate,
        {{bool('forecastenabled')}} as is_forecast_enabled,
        {{set_null('contactid')}} as contact_id,
        {{set_null('accountid')}} as account_id,
        callcenterid,
        extension,
        federationidentifier,
        aboutme,
        loginlimit,
        profilephotoid,
        digestfrequency,
        defaultgroupnotificationfrequency,
        jigsawimportlimitoverride,
        workspaceid,
        sharingtype,
        chatteradoptionstage,
        chatteradoptionstagemodifieddate,
        bannerphotoid,
        {{bool('isprofilephotoactive')}} as is_profile_photo_active,
        individualid,
        globalidentity

    from source

)

select * from renamed
