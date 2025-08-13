with morocco_data as (
    select * 
    from {{ source('electricity_data', 'raw_monthly_electricity_data') }}
    where "Area" = 'Morocco'
)

select 
    dd.date_id as date_id,
    dd.year as year,
    dd.month as month,
    "Date"::date as date,
    "Category" as category,
    "Subcategory" as subcategory,
    "Variable" as variable,
    "Unit" as unit,
    cast("Value" as float) as value
from morocco_data md
join {{ source('electricity_data', 'dim_date') }} dd
on dd.date = md."Date"::date
where "Value" is not null
order by date