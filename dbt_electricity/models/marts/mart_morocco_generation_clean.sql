with monthly_totals as (
    select
        date_id,
        year,
        month,
        energy_type,
        unit,
        sum(value) as monthly_value
    from {{ ref("mart_morocco_generation") }}
    where energy_type in ('Hydro', 'Solar', 'Wind') and unit != '%'
    group by date_id, year, month, energy_type, unit
)

select 
    date_id,
    year,
    month,
    energy_type,
    unit,
    value
from {{ ref("mart_morocco_generation") }}
where energy_type in ('Hydro', 'Solar', 'Wind') and unit != '%'

union all

select
    date_id,
    year,
    month,
    energy_type,
    '%' as unit,
    round(
        (100.0 * mt.monthly_value / sum(mt.monthly_value) over (partition by year, month))::numeric,
        2
    ) as value
from monthly_totals mt
order by year, month, energy_type, unit