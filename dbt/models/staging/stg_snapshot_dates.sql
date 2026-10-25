-- Snapshot dates already applied to the silver history.
select distinct snapshot_date
from {{ source('ops', 'job_runs') }}
where job = 'silver' and status = 'succeeded'
