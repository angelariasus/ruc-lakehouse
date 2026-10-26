"""Weekly upkeep of the Iceberg tables.

    python -m ruc_lakehouse.jobs.maintenance [--keep-days 7] [--bronze-days 14]

Every daily load leaves something behind: silver's merge-on-read writes delete
files and small files, every run appends a tiny file to ``ops.job_runs``, and
each commit or ``CREATE OR REPLACE`` keeps the previous snapshot with its files.
This job, in order:

1. **Bronze retention.** Drops bronze snapshots older than ``--bronze-days``
   before the last date silver applied: they are already in the history and
   can be rebuilt from landing. Bronze would otherwise grow by a full copy of
   the registry (about 310 MB) every day.
2. **Compaction.** Rewrites the data files that carry deletes, merges small
   files and drops the delete files left with nothing to apply to.
3. **Snapshot expiry.** Keeps ``--keep-days`` of time travel on every table and
   deletes the files only older snapshots used: replaced gold tables, rewritten
   silver files, dropped bronze snapshots.
4. **Orphan files.** Deletes files that no metadata points to, such as the
   output of a failed write. Only files older than three days are considered,
   so a write in progress is never touched.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

from ruc_lakehouse.config import Settings
from ruc_lakehouse.jobs.bronze import BRONZE
from ruc_lakehouse.jobs.silver import HISTORY
from ruc_lakehouse.jobs.silver import JOB as SILVER_JOB
from ruc_lakehouse.ops import RUNS, ensure_runs_table, last_succeeded, record_run
from ruc_lakehouse.reporting import materialization_reporter
from ruc_lakehouse.spark import CATALOG, build_session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

JOB = "maintenance"
# Namespaces holding Iceberg tables; staging holds only dbt views.
NAMESPACES = ("bronze", "silver", "ops", "gold", "reference")
# Merge-on-read leaves daily delete files and small files on these.
COMPACTED = (HISTORY, RUNS)
ORPHAN_AGE = timedelta(days=3)
# Smaller than Iceberg's 512 MB default, so silver's ~100 MB files count as
# full size and only files with deletes or small ones are rewritten.
TARGET_FILE_SIZE = 128 * 1024 * 1024


def bronze_cutoff(last_applied: date | None, keep_days: int) -> date | None:
    """Bronze snapshots before this date can go; None while silver is empty."""
    if last_applied is None:
        return None
    return last_applied - timedelta(days=keep_days)


def call(procedure: str, table: str, **args: str) -> str:
    """SQL for an Iceberg procedure on ``table`` (full name, with the catalog)."""
    named = ", ".join(
        [f"table => '{table}'", *(f"{key} => {value}" for key, value in args.items())]
    )
    return f"call {CATALOG}.system.{procedure}({named})"


def options(values: Mapping[str, str]) -> str:
    return "map(" + ", ".join(f"'{key}', '{value}'" for key, value in values.items()) + ")"


def timestamp(moment: datetime) -> str:
    return f"timestamp '{moment.strftime('%Y-%m-%d %H:%M:%S')}'"


def list_tables(spark: SparkSession) -> list[str]:
    existing = {row["namespace"] for row in spark.sql(f"show namespaces in {CATALOG}").collect()}
    tables = []
    for namespace in NAMESPACES:
        if namespace not in existing:
            continue
        for row in spark.sql(f"show tables in {CATALOG}.{namespace}").collect():
            if not row["isTemporary"]:
                tables.append(f"{CATALOG}.{namespace}.{row['tableName']}")
    return sorted(tables)


def table_stats(spark: SparkSession, table: str) -> dict[str, int]:
    """Files of the current snapshot, files kept by any snapshot, and snapshots."""
    live = spark.sql(
        f"select count_if(content = 0) as data, count_if(content <> 0) as deletes "
        f"from {table}.files"
    ).first()
    stored = spark.sql(
        f"select count(*) as files, coalesce(sum(file_size_in_bytes), 0) as bytes "
        f"from (select distinct file_path, file_size_in_bytes from {table}.all_files)"
    ).first()
    snapshots = spark.sql(f"select count(*) as n from {table}.snapshots").first()
    return {
        "data_files": live["data"] if live else 0,
        "delete_files": live["deletes"] if live else 0,
        "stored_files": stored["files"] if stored else 0,
        "stored_bytes": stored["bytes"] if stored else 0,
        "snapshots": snapshots["n"] if snapshots else 0,
    }


def drop_old_bronze(spark: SparkSession, cutoff: date | None) -> int:
    if cutoff is None or not spark.catalog.tableExists(BRONZE):
        return 0
    old = spark.sql(
        f"select count(distinct snapshot_date) as n from {BRONZE} "
        f"where snapshot_date < date '{cutoff.isoformat()}'"
    ).first()
    dropped = old["n"] if old else 0
    if dropped:
        # Whole partitions: Iceberg drops the files without rewriting any.
        spark.sql(f"delete from {BRONZE} where snapshot_date < date '{cutoff.isoformat()}'")
    return dropped


def compact(spark: SparkSession, table: str) -> int:
    rewrite = {
        "delete-file-threshold": "1",
        "target-file-size-bytes": str(TARGET_FILE_SIZE),
        "remove-dangling-deletes": "true",
    }
    row = spark.sql(call("rewrite_data_files", table, options=options(rewrite))).first()
    return row["rewritten_data_files_count"] if row else 0


def expire(spark: SparkSession, table: str, older_than: datetime) -> None:
    spark.sql(call("expire_snapshots", table, older_than=timestamp(older_than))).collect()


def remove_orphans(spark: SparkSession, table: str, older_than: datetime) -> int:
    rows = spark.sql(
        call("remove_orphan_files", table, older_than=timestamp(older_than), prefix_listing="true")
    ).collect()
    return len(rows)


def run(spark: SparkSession, keep_days: int, bronze_days: int) -> dict[str, int]:
    started = datetime.now(UTC)
    ensure_runs_table(spark)
    now = started.replace(tzinfo=None)

    tables = list_tables(spark)
    before = {table: table_stats(spark, table) for table in tables}

    cutoff = bronze_cutoff(last_succeeded(spark, SILVER_JOB), bronze_days)
    metrics = {"bronze_snapshots_dropped": drop_old_bronze(spark, cutoff)}
    metrics["data_files_rewritten"] = sum(
        compact(spark, table) for table in COMPACTED if table in before
    )
    for table in tables:
        expire(spark, table, now - timedelta(days=keep_days))
    metrics["orphan_files_removed"] = sum(
        remove_orphans(spark, table, now - ORPHAN_AGE) for table in tables
    )

    after = {table: table_stats(spark, table) for table in tables}
    for key in ("stored_files", "stored_bytes", "delete_files", "snapshots"):
        metrics[f"{key}_before"] = sum(stats[key] for stats in before.values())
        metrics[f"{key}_after"] = sum(stats[key] for stats in after.values())

    record_run(
        spark,
        job=JOB,
        snapshot_date=started.date(),
        status="succeeded",
        input=f"keep_days={keep_days} bronze_days={bronze_days}",
        metrics=metrics,
        started_at=started,
        finished_at=datetime.now(UTC),
    )
    for table in tables:
        b, a = before[table], after[table]
        name = table.removeprefix(f"{CATALOG}.")
        print(
            f"{name}: {b['stored_files']} -> {a['stored_files']} files, "
            f"{b['stored_bytes'] / 1e6:,.1f} -> {a['stored_bytes'] / 1e6:,.1f} MB, "
            f"{b['delete_files']} -> {a['delete_files']} delete files, "
            f"{b['snapshots']} -> {a['snapshots']} snapshots"
        )
    print(", ".join(f"{key}={value:,}" for key, value in metrics.items()))
    return metrics


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--keep-days", type=int, default=7, help="days of time travel to keep")
    parser.add_argument(
        "--bronze-days", type=int, default=14, help="days of bronze kept before silver's last date"
    )
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    with materialization_reporter() as report:
        spark = build_session("maintenance", settings)
        try:
            report(run(spark, args.keep_days, args.bronze_days))
        finally:
            spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
