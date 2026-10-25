-- Current state of every RUC in the registry. Names of natural persons are
-- left out: gold only names legal entities.
with versions as (
    select * from {{ ref('stg_taxpayer_versions') }}
),

first_seen as (
    select
        ruc,
        min(valid_from) as first_seen_on
    from versions
    group by ruc
)

select
    v.ruc,
    v.taxpayer_type,
    case when v.taxpayer_type = 'legal_entity' then v.name end as name,
    v.taxpayer_status,
    v.status_group,
    v.address_condition,
    v.ubigeo,
    u.department,
    u.province,
    u.district,
    f.first_seen_on,
    v.valid_from as current_since
from versions as v
inner join first_seen as f on v.ruc = f.ruc
left join {{ ref('dim_ubigeo') }} as u on v.ubigeo = u.ubigeo
where v.is_current
