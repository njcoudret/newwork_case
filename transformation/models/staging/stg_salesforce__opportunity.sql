

with source as (

    select * from {{ source('salesforce', 'opportunity') }}

),

renamed as (

    select
        id as opportunity_id,
        {{bool('isdeleted')}} as is_deleted,
        {{set_null('accountid')}} as account_id,
        {{bool('isprivate')}} as is_private,
        name,
        description,
        stagename as stage_name,
        regexp_replace(lower(stagename),'[./\s]+','_','g') as stage_name_underscored,
        stagesortorder as stage_sort_order,
        amount,
        probability,
        expectedrevenue as revenue_expected,
        totalopportunityquantity as quantity,
        {{c_date('closedate')}} as date_close,
        CASE 
            WHEN type LIKE 'New Customer%' THEN 'New'
            WHEN type LIKE 'Existing Customer%' THEN 'Existing'
            ELSE 'Other'
        END as new_existing_customer,
        CASE source.type
            WHEN type LIKE '%Upgrade' THEN 'Upgrade'
            WHEN type LIKE '%Replacement' THEN 'Replacement'
            WHEN type LIKE 'Existing Customer%' THEN 'Other'
            ELSE NULL
        END existing_customer_category,
        nextstep,
        leadsource as source,
        {{bool('isclosed')}} as is_closed,
        {{bool('iswon')}} as is_won,
        forecastcategory as forecast_category,
        forecastcategoryname as forecast_category_name,
        {{set_null('campaignid')}} as campaign_id,
        {{bool('hasopportunitylineitem')}} has_opportunity_line_item,
        {{set_null('pricebook2id')}} as price_book_id,
        {{set_null('ownerid')}} as user_id_owner,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        {{to_local_tz('lastactivitydate')}} as datetime_last_activity,
        laststagechangedate,
        fiscalyear as year_fiscal,
        fiscalquarter as quarter_fiscal,
        {{set_null('contactid')}} as contact_id,
        {{set_null('primarypartneraccountid')}} as account_id_primary_partner,
        {{set_null('contractid')}} as contract_id,
        {{set_null('lastamountchangedhistoryid')}} as last_amount_changed_history_id,
        {{set_null('lastclosedatechangedhistoryid')}} as last_close_date_changed_history_id,
        deliveryinstallationstatus__c as status_delivery_installation,
        trackingnumber__c as tracking_number,
        ordernumber__c as order_number,
        currentgenerators__c as current_generators,
        maincompetitors__c as main_competitors

    from source

)

select * from renamed
