with contacts as (

    select * from {{ ref('stg_salesforce__contact') }}

),

country_aliases as (

    select * from {{ ref('seed_country_aliases')}}
),

fix_country_names as (
    select 
        c.contact_id,
        coalesce(
            ca_b_2.clean,
            ca_b.clean
        ) as country_mailing  
    from contacts as c
    left join country_aliases as ca_b
        on c.street_mailing like concat('%',ca_b.alias)
    left join country_aliases as ca_b_2
        on c.country_mailing = ca_b_2.alias

)

select * from fix_country_names