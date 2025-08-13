select 
    date_id,
    year,
    month,
    energy_type,
    unit,
    value
from {{ ref("stg_morocco_generation") }}  
where energy_category = 'Fuel'
order by date_id, energy_type, unit