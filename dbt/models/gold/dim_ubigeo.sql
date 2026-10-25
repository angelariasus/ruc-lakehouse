-- Peru's districts (INEI, 1,891 districts), from the ubigeo seed.
select
    ubigeo,
    department,
    province,
    district,
    natural_region
from {{ ref('ubigeo') }}
