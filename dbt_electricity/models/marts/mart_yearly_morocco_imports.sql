select
    year,
    unit,
    round(sum(value)::numeric, 2) as total_import
from {{ ref("stg_morocco_imports") }}
group by 1, 2
order by 1