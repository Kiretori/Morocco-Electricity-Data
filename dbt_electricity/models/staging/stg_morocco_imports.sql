select 
    date_id,
    year,
    month,
    unit,
    value
from {{ ref("stg_morocco_raw")}}
where category = 'Electricity imports'
order by date_id