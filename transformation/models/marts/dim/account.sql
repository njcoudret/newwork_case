with real_accounts as (
    select * from {{ ref('stg_salesforce__account')}}
    where 
        is_real_account
        and not is_deleted
),

primary_contact as (
    select 
        pca.account_id,
        pca.contact_id
    from {{ ref('int_primary_contact_account')}} as pca
),


accounts_with_countries as (
    select * from {{ ref('int_accounts_with_countries')}}
),


combined_data as (
    select
        a.account_id,
        a.datetime_create,
        a.name,
        a.type,
        a.state_billing,
        ac.country_billing,
        a.state_shipping,
        ac.country_shipping,
        a.industry,
        a.nr_employees,
        a.nr_locations,
        a.revenue_annual,
        a.ownership,
        a.user_id_owner,
        a.user_id_create,
        a.user_id_last_modified,
        a.is_active,
        a.rating,
        a.sla_category,
        a.customer_priority,
        pc.contact_id as contact_id_primary
    from real_accounts as a
    left join primary_contact as pc
        on a.account_id = pc.account_id
    left join accounts_with_countries as ac
        on a.account_id = ac.account_id
)

select * from combined_data