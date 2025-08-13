select
    extract(year from date) as year,
    unit,
    round(sum(value)::numeric, 2) as total_demand
from {{ ref("stg_morocco_raw") }}
where category = 'Electricity demand'
group by 1, 2
order by 1