

with source as (

    select * from {{ source('salesforce', 'case') }}

),

renamed as (

    select
        id as case_id,
        {{bool('isdeleted')}} as is_deleted,
        {{set_null('masterrecordid')}} as master_record_id,
        casenumber,
        {{set_null('contactid')}} as contact_id,
        {{set_null('accountid')}} as account_id,
        {{set_null('assetid')}} as asset_id,
        {{set_null('productid')}} as product_id,
        {{set_null('entitlementid')}} as entitlement_id,
        {{set_null('sourceid')}} as source_id,
        {{set_null('businesshoursid')}} as businesshours_id,
        {{set_null('parentid')}} as parent_id,
        suppliedname as name,
        suppliedemail as email,
        suppliedphone as phone,
        suppliedcompany as company,
        type,
        status,
        reason,
        origin,
        subject,
        priority,
        case
            when lower(priority) = 'high' then 1
            when lower(priority) = 'medium' then 2
            when lower(priority) = 'low' then 3
            else 4
        end as priority_order,
        description,
        {{bool('isclosed')}} as is_closed,
        {{to_local_tz('closeddate')}} as datetime_closed,
        {{bool('isescalated')}} as is_escalated,
        {{set_null('ownerid')}} as user_id_owner,
        {{bool('isclosedoncreate')}} as is_closed_on_create,
        slastartdate,
        slaexitdate,
        {{bool('isstopped')}} as is_stopped,
        stopstartdate,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        {{set_null('servicecontractid')}} as service_contract_id,
        {{to_local_tz('eventsprocesseddate')}} as datetime_events_processed,
        engineeringreqnumber__c as engineering_req_number,
        {{bool('slaviolation__c')}} as is_sla_violation,
        product__c as product_code,
        {{bool('potentialliability__c')}} as is_potential_liability

    from source

)

select * from renamed
