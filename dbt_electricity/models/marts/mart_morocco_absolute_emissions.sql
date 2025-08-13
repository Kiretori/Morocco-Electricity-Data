select
    date_id, 
    year,
    month,
    emission_type,
    unit,
    value
from {{ ref('stg_morocco_emissions') }}
where unit = 'mtCO2'
order by date_id