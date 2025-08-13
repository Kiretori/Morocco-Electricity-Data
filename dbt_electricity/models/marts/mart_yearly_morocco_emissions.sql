select
    year,
    case 
        when unit = 'gCO2/kWh' then 'CO2 intensity'
        else 'Absolute emissions' 
    end as emission_type,
    unit,
    round(sum(value)::numeric, 2) as total_emissions
from {{ ref("stg_morocco_emissions") }}
group by year, unit
order by year