select
    year,
    unit,
    round(sum(value)::numeric, 2) as total_generated
from {{ ref("mart_morocco_generation") }}
where unit != '%'
group by year, unit
order by year