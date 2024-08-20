

with source as (

    select * from {{ source('salesforce', 'account') }}

),

renamed as (

    select
        id as account_id,
        {{bool('isdeleted')}} as is_deleted,
        if(
            name = 'Sample Account for Entitlements',
            false,
            true
        ) as is_real_account,
        {{set_null('masterrecordid')}} as master_record_id,
        name,
        REPLACE(type,'Customer - ','') as type,
        {{set_null('parentid')}} as parent_id,
        billingstreet as street_billing,
        billingcity as city_billing,
        billingstate as state_billing,
        billingpostalcode as postalcode_billing,
        billingcountry as country_billing,
        billinglatitude as latitude_billing,
        billinglongitude as longitude_billing,
        billinggeocodeaccuracy as geocodeaccuracy_billing,
        shippingstreet as street_shipping,
        shippingcity as city_shipping,
        shippingstate as state_shipping,
        shippingpostalcode as postalcode_shipping,
        shippingcountry as country_shipping,
        shippinglatitude as latitude_shipping,
        shippinglongitude as longitude_shipping,
        shippinggeocodeaccuracy as geocodeaccuracy_shipping,
        phone,
        fax,
        accountnumber,
        website,
        sic,
        industry,
        annualrevenue as revenue_annual,
        numberofemployees as nr_employees,
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
        numberoflocations__c as nr_locations,
        upsellopportunity__c as upsell_opportunity,
        slaserialnumber__c as sla_serial_number,
        {{c_date('slaexpirationdate__c')}} AS date_sla_expiration
    from source

)

select * from renamed
