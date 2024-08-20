with relevant_opportunities as (
    select * from {{ ref('stg_salesforce__opportunity') }}
    where not is_deleted
),

users as (
    select * from {{ ref('stg_salesforce__user')}}
),

selected_columns as (
    select 
        o.opportunity_id,
        o.account_id,
        o.name,
        o.stage_name,
        o.stage_sort_order,
        o.date_close,
        o.new_existing_customer,
        o.existing_customer_category,
        o.source,
        o.is_closed,
        o.is_won,
        u_owner.full_name as full_name_owner,
        o.status_delivery_installation,
        o.current_generators,
        o.main_competitors,
        1 as nr_opportunities,
        o.amount,
        o.probability,
        o.revenue_expected,
        o.quantity
    from relevant_opportunities o
    left join users as u_owner
        on o.user_id_owner = u_owner.user_id
)

select * from selected_columns