-- The silver history must never hold two current versions of the same RUC.
select ruc
from {{ source('silver', 'taxpayers_history') }}
where is_current
group by ruc
having count(*) > 1
