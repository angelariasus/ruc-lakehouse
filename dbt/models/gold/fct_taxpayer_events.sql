-- One row per change seen in the registry, on the snapshot date it was seen:
-- registered, removed, or a change of status, address condition, location,
-- name or address. Status, condition and location changes keep the value
-- before and after.
with versions as (
    select * from {{ ref('stg_taxpayer_versions') }}
),

changed_rucs as (
    select distinct ruc
    from versions
    where change_type = 'changed'
),

-- The previous version is only needed for RUCs that changed.
paired as (
    select
        v.*,
        lag(v.taxpayer_status) over (partition by v.ruc order by v.valid_from)
            as previous_status,
        lag(v.address_condition) over (partition by v.ruc order by v.valid_from)
            as previous_address_condition,
        lag(v.ubigeo) over (partition by v.ruc order by v.valid_from) as previous_ubigeo
    from versions as v
    where v.ruc in (select c.ruc from changed_rucs as c)
),

registered as (
    select
        valid_from as snapshot_date,
        ruc,
        taxpayer_type,
        ubigeo,
        'registered' as event_type,
        cast(null as varchar) as from_value,
        taxpayer_status as to_value
    from versions
    where change_type = 'new'
),

removed as (
    select
        valid_to as snapshot_date,
        ruc,
        taxpayer_type,
        ubigeo,
        'removed' as event_type,
        taxpayer_status as from_value,
        cast(null as varchar) as to_value
    from versions
    where end_reason = 'removed'
),

changed as (
    select
        p.valid_from as snapshot_date,
        p.ruc,
        p.taxpayer_type,
        p.ubigeo,
        case c.column_name
            when 'taxpayer_status' then 'status_changed'
            when 'address_condition' then 'address_condition_changed'
            when 'ubigeo' then 'location_changed'
            when 'name' then 'name_changed'
            else 'address_changed'
        end as event_type,
        case c.column_name
            when 'taxpayer_status' then p.previous_status
            when 'address_condition' then p.previous_address_condition
            when 'ubigeo' then p.previous_ubigeo
        end as from_value,
        case c.column_name
            when 'taxpayer_status' then p.taxpayer_status
            when 'address_condition' then p.address_condition
            when 'ubigeo' then p.ubigeo
        end as to_value
    from paired as p
    cross join unnest(p.changed_columns) as c (column_name)
    where p.change_type = 'changed'
)

-- Several address columns changing at once count as one address change.
select distinct
    snapshot_date,
    ruc,
    taxpayer_type,
    ubigeo,
    event_type,
    from_value,
    to_value
from (
    select * from registered
    union all
    select * from removed
    union all
    select * from changed
) as events
