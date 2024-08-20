with accounts as (
    select * from {{ ref('stg_salesforce__account') }}
),

country_aliases as (

    select * from {{ ref('seed_country_aliases')}}
),

fix_country_names as (
    select 
        a.account_id,
        coalesce(
            ca_b_2.clean,
            ca_b.clean
        ) as country_billing,
        coalesce(
            ca_s_2.clean,
            ca_s.clean
        ) as country_shipping,     
    from accounts as a
    left join country_aliases as ca_b
        on a.street_billing like concat('%',ca_b.alias)
    left join country_aliases as ca_b_2
        on a.country_billing = ca_b_2.alias
    left join country_aliases as ca_s
        on a.street_shipping like concat('%',ca_s.alias)
    left join country_aliases as ca_s_2
        on a.country_shipping = ca_s_2.alias

)

select * from fix_country_names