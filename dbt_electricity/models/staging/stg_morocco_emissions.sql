select
    date_id,
    year,
    month,
    variable as emission_type,
    unit,
    value
from {{ ref('stg_morocco_raw')}}
where
    category = 'Power sector emissions'
    and
    subcategory not in ('Aggregate fuel', 'Total')
order by date_id