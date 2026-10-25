-- How taxpayers move between statuses, e.g. ACTIVO -> BAJA DE OFICIO, per day.
select
    snapshot_date,
    taxpayer_type,
    from_value as from_status,
    to_value as to_status,
    count(*) as taxpayers
from {{ ref('fct_taxpayer_events') }}
where event_type = 'status_changed'
group by 1, 2, 3, 4
