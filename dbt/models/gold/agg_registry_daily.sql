-- Size of the registry on every applied snapshot date, by taxpayer type,
-- status, address condition and department.
--
-- Instead of re-reading the whole history for every date, each version adds 1
-- to its group on valid_from and subtracts 1 on valid_to; a running sum over
-- the dates gives the count on each date.
with versions as (
    select
        v.taxpayer_type,
        v.status_group,
        coalesce(v.address_condition, '(not reported)') as address_condition,
        case
            when v.ubigeo is null then '(not reported)'
            else coalesce(u.department, '(unknown code)')
        end as department,
        v.valid_from,
        v.valid_to
    from {{ ref('stg_taxpayer_versions') }} as v
    left join {{ ref('dim_ubigeo') }} as u on v.ubigeo = u.ubigeo
),

deltas as (
    select
        valid_from as snapshot_date,
        taxpayer_type,
        status_group,
        address_condition,
        department,
        1 as delta
    from versions
    union all
    select
        valid_to as snapshot_date,
        taxpayer_type,
        status_group,
        address_condition,
        department,
        -1 as delta
    from versions
    where valid_to is not null
),

daily_deltas as (
    select
        snapshot_date,
        taxpayer_type,
        status_group,
        address_condition,
        department,
        sum(delta) as delta
    from deltas
    group by 1, 2, 3, 4, 5
),

groups as (
    select distinct
        taxpayer_type,
        status_group,
        address_condition,
        department
    from daily_deltas
),

-- Every group on every date, so a group without changes keeps its count.
grid as (
    select
        d.snapshot_date,
        g.taxpayer_type,
        g.status_group,
        g.address_condition,
        g.department
    from {{ ref('stg_snapshot_dates') }} as d
    cross join groups as g
),

running as (
    select
        g.snapshot_date,
        g.taxpayer_type,
        g.status_group,
        g.address_condition,
        g.department,
        sum(coalesce(dd.delta, 0)) over (
            partition by g.taxpayer_type, g.status_group, g.address_condition, g.department
            order by g.snapshot_date
            rows between unbounded preceding and current row
        ) as taxpayers
    from grid as g
    left join daily_deltas as dd
        on
            g.snapshot_date = dd.snapshot_date
            and g.taxpayer_type = dd.taxpayer_type
            and g.status_group = dd.status_group
            and g.address_condition = dd.address_condition
            and g.department = dd.department
)

select *
from running
where taxpayers > 0
