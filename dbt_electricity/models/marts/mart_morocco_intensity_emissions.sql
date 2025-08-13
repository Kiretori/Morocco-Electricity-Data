select
    date_id, 
    year,
    month,
    unit,
    value
from {{ ref('stg_morocco_emissions') }}
where unit = 'gCO2/kWh'
order by date_id