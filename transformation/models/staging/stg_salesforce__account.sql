

with source as (

    select * from {{ source('salesforce', 'account') }}

),

renamed as (

    select
        id as account_id,
        {{bool('isdeleted')}} as is_deleted,
        {{set_null('masterrecordid')}} as master_record_id,
        name,
        REPLACE(type,'Customer - ','') as type,
        {{set_null('parentid')}} as parent_id,
        billingstreet,
        billingcity,
        billingstate,
        billingpostalcode,
        billingcountry,
        billinglatitude,
        billinglongitude,
        billinggeocodeaccuracy,
        shippingstreet,
        shippingcity,
        shippingstate,
        shippingpostalcode,
        shippingcountry,
        shippinglatitude,
        shippinglongitude,
        shippinggeocodeaccuracy,
        phone,
        fax,
        accountnumber,
        website,
        sic,
        industry,
        annualrevenue,
        numberofemployees,
        ownership,
        tickersymbol,
        description,
        rating,
        site,
        {{set_null('ownerid')}} as user_id_owner,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        {{to_local_tz('lastactivitydate')}} as datetime_last_activity,
        jigsaw,
        jigsawcompanyid,
        cleanstatus as status,
        accountsource,
        dunsnumber,
        tradestyle,
        naicscode,
        naicsdesc,
        yearstarted,
        sicdesc,
        {{set_null('dandbcompanyid')}} as dandbcompany_id,
        {{set_null('operatinghoursid')}} as operatinghours_id,
        customerpriority__c as customer_priority,
        sla__c as sla_category,
        {{bool('active__c')}} as is_active,
        numberoflocations__c as number_of_locations,
        upsellopportunity__c as upsell_opportunity,
        slaserialnumber__c as sla_serial_number,
        {{c_date('slaexpirationdate__c')}} AS date_sla_expiration

    from source

)

select * from renamed
