# Operations

How the lakehouse runs day to day, how to tell it is healthy, and what to do
when a check stops it.

## What runs on its own

| When | What | Where |
|---|---|---|
| 09:00 and 21:00, Lima time | The collector downloads SUNAT's zip if it changed and adds a line to `manifest.jsonl` | Windows scheduled task, outside the repo |
| Every 5 minutes | `new_snapshot_sensor` finds new snapshots in the manifest and queues `ingest_snapshot` for each, oldest first | Dagster |
| After each successful ingestion | `gold_after_ingestion` queues `build_gold` (`dbt build`) | Dagster |
| Sundays, 04:00 Lima time | `weekly_maintenance` runs `maintain_tables` | Dagster |
| Daily | Staging files older than 7 days expire | Storage lifecycle rule |

Only one run executes at a time; the others wait in the queue in the order
they were requested.

## Is it healthy?

- **Freshness.** In Dagster, `silver/taxpayers_history` warns when its last
  load is more than 30 hours old and fails after 48. A failure usually means
  the collector stopped (look at its log in the snapshots folder) or a load
  failed (look at the run in Dagster).
- **Data tests.** dbt tests show up as asset checks on the gold models.
- **The run log.** Every job writes one row to `ops.job_runs` with its metrics.
  From Trino:

```sql
-- Recent runs and how long they took
select job, snapshot_date, status, date_diff('second', started_at, finished_at) as seconds, message
from lakehouse.ops.job_runs
order by started_at desc
limit 20;

-- Daily changes applied to the history
select snapshot_date, metrics['new'] as new, metrics['changed'] as changed,
       metrics['removed'] as removed
from lakehouse.ops.job_runs
where job = 'silver' and status = 'succeeded'
order by snapshot_date desc;

-- What each maintenance pass reclaimed
select started_at, metrics['stored_bytes_before'] - metrics['stored_bytes_after'] as bytes_freed,
       metrics['data_files_rewritten'] as files_rewritten
from lakehouse.ops.job_runs
where job = 'maintenance'
order by started_at desc;
```

## When a check stops a load

No check leaves a table half-changed. Landing keeps the raw zip (it is the
history) but writes no completion marker, so bronze will not read the day.
Bronze and silver change nothing but add a `failed` row, with the reason, to
`ops.job_runs`.

| Error | Raised by | Meaning | What to do |
|---|---|---|---|
| `SchemaDriftError` | landing | SUNAT changed the file header | Update the contract in `contracts.py` and the bronze table, then re-run |
| `ContractViolation` | bronze | More than 0.1% of lines rejected, or the line count differs from landing | Look at `bronze.padron_ruc_rejects` for the date; a new format quirk needs a parser fix |
| `SuspiciousSnapshot` | silver | The day would remove more than 1% of current RUCs, probably a truncated file | Compare the zip's size and line count with earlier days. A truncated file is usually fixed in SUNAT's next publication, which is compared with the last applied state. If the drop is real, such as a mass deregistration, raise `MAX_REMOVED_RATIO` in `silver.py` for that run |
| `OutOfOrderSnapshot` | silver | A date older than the last applied one | Nothing: the history stays consistent. The day stays in bronze |
| `bronze has no rows` | silver | Bronze has no partition for the date | Run landing and bronze for the date first |

## Running things by hand

The jobs are command-line programs; Dagster only runs them. With the stack up:

```bash
# Re-run one day. Landing skips a file it already has, bronze replaces the
# day's partition, and silver does nothing for a date it already applied.
docker compose run --rm jobs python -m ruc_lakehouse.ingest.landing 2026-10-26
docker compose run --rm jobs python -m ruc_lakehouse.jobs.bronze 2026-10-26
docker compose run --rm jobs python -m ruc_lakehouse.jobs.silver 2026-10-26
docker compose run --rm jobs dbt build

# Maintenance, with the defaults the schedule uses.
docker compose run --rm jobs python -m ruc_lakehouse.jobs.maintenance --keep-days 7 --bronze-days 14
```

Rebuilding the history from scratch means dropping
`silver.taxpayers_history`, deleting the `silver` rows from `ops.job_runs`, and
applying every date in order. Bronze keeps only the last two weeks, so older
dates have to be loaded again from landing first.

## Table maintenance

[`jobs/maintenance.py`](../src/ruc_lakehouse/jobs/maintenance.py) runs four
steps each week:

1. **Bronze retention.** Drops bronze days older than `--bronze-days` (14)
   before the last date silver applied. They are already in the history, and
   landing can rebuild them.
2. **Compaction.** On silver and `ops.job_runs`, rewrites the files that have
   delete files and merges small files, then drops the delete files that no
   longer apply. Silver uses merge-on-read, so every day adds delete files that
   slow reads until compaction folds them in.
3. **Snapshot expiry.** On every table, keeps `--keep-days` (7) of time travel
   and deletes the files only older snapshots used: replaced gold tables,
   rewritten silver files and dropped bronze days.
4. **Orphan files.** Deletes files no metadata points to and older than three
   days, such as the output of a failed write.

On the real lake, the first pass took 20 seconds and freed 310 MB from a
re-run of bronze and 115 MB from a replaced gold table. In CI, it folds day 2's
delete files into silver without changing a row.

## Disk use on this machine

Locally there is no cheaper storage class, so landing grows by about 11 GB a
month, plus the collector's own copy of each zip. Staging (440 MB a day) expires
after a week. Bronze stays at about two weeks of snapshots (310 MB each) once
maintenance runs.
