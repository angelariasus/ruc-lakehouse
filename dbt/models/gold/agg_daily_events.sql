-- Events per snapshot date, by type of event, taxpayer type and department.
select
    e.snapshot_date,
    e.event_type,
    e.taxpayer_type,
    case
        when e.ubigeo is null then '(not reported)'
        else coalesce(u.department, '(unknown code)')
    end as department,
    count(*) as events
from {{ ref('fct_taxpayer_events') }} as e
left join {{ ref('dim_ubigeo') }} as u on e.ubigeo = u.ubigeo
group by 1, 2, 3, 4
