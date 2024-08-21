with relevant_opportunities as (
    select * from {{ ref('stg_salesforce__opportunity') }}
    where not is_deleted
),

selected_columns as (
    select 
        o.opportunity_id,
        o.account_id,
        o.name,
        o.stage_name,
        o.stage_sort_order,
        o.stage_name_underscored,
        o.date_close,
        o.new_existing_customer,
        o.existing_customer_category,
        o.source,
        o.is_closed,
        o.is_won,
        o.user_id_owner,
        o.user_id_create,
        o.user_id_last_modified,
        o.status_delivery_installation,
        o.current_generators,
        o.main_competitors,
        1 as nr_opportunities,
        o.amount,
        o.probability,
        o.revenue_expected,
        o.quantity
    from relevant_opportunities o
)

select * from selected_columns