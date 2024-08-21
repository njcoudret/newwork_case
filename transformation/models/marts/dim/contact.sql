with relevant_contacts as (
    select * from {{ ref('stg_salesforce__contact')}}
    where not is_deleted
),

contacts_with_countries as (
    select * from {{ ref('int_contacts_with_countries')}}
),

selected_columns as (
    select
        c.contact_id,
        c.account_id,
        c.salutation,
        c.first_name,
        c.last_name,
        c.full_name,
        cc.country_mailing,
        c.mobilephone,
        c.email,
        c.title,
        c.department,
        c.lead_source,
        c.date_birth,
        c.contact_level
    from relevant_contacts as c
    left join contacts_with_countries cc
        on c.contact_id = cc.contact_id
)

select * from selected_columns