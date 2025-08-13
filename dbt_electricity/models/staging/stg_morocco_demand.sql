select 
    date_id,
    year,
    month,
    unit,
    value
from {{ ref("stg_morocco_raw")}}
where category = 'Electricity demand'
order by date_id