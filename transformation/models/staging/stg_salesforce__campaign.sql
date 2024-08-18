

with source as (

    select * from {{ source('salesforce', 'campaign') }}

),

renamed as (

    select
        id as campaign_id,
        {{bool('isdeleted')}} as is_deleted,
        name,
        {{set_null('parentid')}} as parent_id,
        type,
        status,
        {{c_date('startdate')}} as date_start,
        {{c_date('enddate')}} as date_end,
        expectedrevenue as revenue_expected,
        budgetedcost as cost_budget,
        actualcost as cost_actual,
        expectedresponse as response_expected,
        numbersent as nr_sent,
        {{bool('isactive')}} as is_active,
        description,
        numberofleads as nr_leads,
        numberofconvertedleads as nr_leads_converted,
        numberofcontacts as nr_contacts,
        numberofresponses as nr_responses,
        numberofopportunities as nr_opportunities,
        numberofwonopportunities as nr_opportunities_won,
        amountallopportunities as amt_opportunities,
        amountwonopportunities as amt_opportunities_won,
        hierarchynumberofleads as nr_leads_hierarchy,
        hierarchynumberofconvertedleads as nr_leads_converted_hierarchy,
        hierarchynumberofcontacts as nr_contacts_hierarchy,
        hierarchynumberofresponses as nr_responses_hierarchy,
        hierarchynumberofopportunities as nr_opportunities_hierarchy,
        hierarchynumberofwonopportunities as nr_opportunities_won_hierarchy,
        hierarchyamountallopportunities as amt_opportunities_hierarchy,
        hierarchyamountwonopportunities as amt_opportunities_won_hierarchy,
        hierarchynumbersent as nr_sent_hierarchy,
        hierarchyexpectedrevenue as revenue_expected_hierarchy,
        hierarchybudgetedcost as cost_budget_hierarchy,
        hierarchyactualcost as cost_actual_hierarchy,
        {{set_null('ownerid')}} as user_id_owner,
        {{to_local_tz('createddate')}} as datetime_create,
        {{set_null('createdbyid')}} as user_id_create,
        {{to_local_tz('lastmodifieddate')}} as datetime_last_modified,
        {{set_null('lastmodifiedbyid')}} as user_id_last_modified,
        {{to_local_tz('systemmodstamp')}} as datetime_system_mod,
        {{to_local_tz('lastactivitydate')}} as datetime_last_activity,
        campaignmemberrecordtypeid

    from source

)

select * from renamed
