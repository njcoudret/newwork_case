with relevant_contacts as (
    select * from {{ ref('stg_salesforce__contact')}}
    where not is_deleted
),

one_contact_per_account as (
    select
        account_id,
        contact_id,
        row_number() over (partition by account_id order by datetime_last_modified) as rn
    from relevant_contacts
    where contact_level = 'Primary'
)

select * from one_contact_per_account
where rn = 1