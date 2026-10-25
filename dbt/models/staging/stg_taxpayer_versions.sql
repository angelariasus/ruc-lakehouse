-- Every version of every RUC, with the derived attributes gold reports on.
-- RUCs starting with 10, 15 or 17 belong to natural persons, 20 to legal
-- entities.
with versions as (
    select * from {{ source('silver', 'taxpayers_history') }}
)

select
    ruc,
    case
        when substr(ruc, 1, 2) = '20' then 'legal_entity'
        when substr(ruc, 1, 2) in ('10', '15', '17') then 'natural_person'
        else 'other'
    end as taxpayer_type,
    name,
    taxpayer_status,
    case
        when taxpayer_status = 'ACTIVO' then 'active'
        when taxpayer_status like 'SUSPENSION%' then 'suspended'
        when taxpayer_status like 'BAJA%' then 'deregistered'
        else 'other'
    end as status_group,
    address_condition,
    ubigeo,
    valid_from,
    valid_to,
    is_current,
    change_type,
    changed_columns,
    end_reason
from versions
