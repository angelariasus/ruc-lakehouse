-- Gold names only legal entities; natural persons stay anonymous.
select ruc
from {{ ref('dim_taxpayer') }}
where taxpayer_type <> 'legal_entity' and name is not null
