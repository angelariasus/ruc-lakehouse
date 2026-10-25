-- On the latest applied date, the daily registry must add up to the number
-- of current versions in silver.
with latest as (
    select sum(taxpayers) as taxpayers
    from {{ ref('agg_registry_daily') }}
    where snapshot_date = (select max(d.snapshot_date) from {{ ref('stg_snapshot_dates') }} as d)
),

current_versions as (
    select count(*) as taxpayers
    from {{ source('silver', 'taxpayers_history') }}
    where is_current
)

select
    l.taxpayers as registry,
    c.taxpayers as current_versions
from latest as l
cross join current_versions as c
where l.taxpayers <> c.taxpayers
