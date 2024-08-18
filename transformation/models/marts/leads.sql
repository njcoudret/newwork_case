with leads as (

    select * from {{ ref('stg_salesforce__lead') }}
    where not is_deleted

),

selected_columns as (
    select
        company,
        state,
        country,
        source,
        status,
        status_type,
        industry,
        rating,
        current_generators,
        product_series,
        COUNT(distinct lead_id) as nr_leads,
        SUM(nr_locations) as nr_locations,
        SUM(revenue_annual) as amt_revenue_annual
    from leads
    group by
        company,
        state,
        country,
        source,
        status,
        status_type,
        industry,
        rating,
        current_generators,
        product_series
)

select * from selected_columns