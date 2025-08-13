select 
    date_id,
    year,
    month,
    subcategory as energy_category,
    variable as energy_type,
    unit,
    value
from {{ ref("stg_morocco_raw")}}
where category = 'Electricity generation'
order by date_id