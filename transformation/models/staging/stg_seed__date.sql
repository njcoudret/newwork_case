with dates as (
    select * from {{ ref('seed_dates_2015_2041')}}
),

date_transformations as (
    select
        date,
        date_part('year',date) as year_nr,
        date_part('isoyear',date) as iso_year_nr,
        date_part('quarter',date) as quarter_nr,
        date_part('month',date) as month_nr,
        date_part('week',date) as iso_week_nr,
        date_part('doy',date) as day_of_year_nr,
        date_part('day',date) as day_of_month,
        date_trunc('year',date) as year_date,
        date_trunc('isoyear',date) as iso_year_date,
        date_trunc('quarter',date) as quarter_date,
        date_trunc('month',date) as quarter_date,
        date_trunc('week',date) as week_date
    from dates
)

select * from date_transformations