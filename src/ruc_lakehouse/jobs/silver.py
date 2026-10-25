"""Apply one bronze snapshot to the taxpayer history (SCD type 2).

    python -m ruc_lakehouse.jobs.silver 2026-10-26

``silver.taxpayers_history`` keeps one row per version of each RUC:

- ``valid_from``: snapshot in which the version was first seen;
- ``valid_to``: snapshot in which it was no longer seen (null while current);
- ``change_type``: why the version opened (``initial``, ``new``, ``changed``);
- ``changed_columns``: for ``changed``, the attributes that differ;
- ``end_reason``: why it closed (``changed``, or ``removed`` when the RUC left
  the registry).

The snapshot is compared with the current versions through a hash of the
tracked attributes, computed on both sides at run time: stored, a SHA-256 per
row would triple the table, since hashes do not compress. Only new, changed
and removed RUCs go into a single MERGE.

Dates are applied in order: applying the last date again is a no-op, and an
earlier date is refused. A snapshot that would remove more than
``MAX_REMOVED_RATIO`` of the current RUCs is refused too: that looks like a
truncated file, not a real day.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from ruc_lakehouse import contracts
from ruc_lakehouse.config import Settings
from ruc_lakehouse.jobs.bronze import BRONZE
from ruc_lakehouse.ops import ensure_runs_table, last_succeeded, record_run
from ruc_lakehouse.spark import CATALOG, build_session

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

HISTORY = f"{CATALOG}.silver.taxpayers_history"
JOB = "silver"
TRACKED: tuple[str, ...] = tuple(c for c in contracts.COLUMNS if c != "ruc")
MAX_REMOVED_RATIO = 0.01
# Separators that cannot appear in the source text, so ("ab", "c") and
# ("a", "bc") hash differently and a null differs from an empty string.
_SEPARATOR = "\x1f"
_NULL = "\x00"

# Daily changes land on scattered RUCs, so copy-on-write would rewrite almost
# every file each day. Merge-on-read writes only the changes plus delete files;
# compaction merges them later.
TABLE_PROPERTIES = (
    "tblproperties ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd', "
    "'write.merge.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
    "'write.delete.mode' = 'merge-on-read')"
)


class OutOfOrderSnapshot(RuntimeError):
    """The snapshot is older than the last one applied to the history."""


class SuspiciousSnapshot(RuntimeError):
    """The snapshot would remove too many taxpayers to be a normal day."""


def check_removed(removed: int, current: int) -> None:
    if current and removed / current > MAX_REMOVED_RATIO:
        raise SuspiciousSnapshot(
            f"{removed:,} of {current:,} current RUCs would be removed, "
            f"above {MAX_REMOVED_RATIO:.0%}"
        )


def with_row_hash(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    values = [F.coalesce(F.col(c), F.lit(_NULL)) for c in TRACKED]
    return df.withColumn("row_hash", F.sha2(F.concat_ws(_SEPARATOR, *values), 256))


def diff(snapshot: DataFrame, current: DataFrame) -> DataFrame:
    """One row per RUC that is new, changed or removed in ``snapshot``.

    ``snapshot`` has the bronze columns and ``current`` the current history
    versions. Unchanged RUCs are left out.
    """
    from pyspark.sql import functions as F

    new = with_row_hash(snapshot.select("ruc", *TRACKED)).alias("s")
    old = with_row_hash(current.select("ruc", *TRACKED)).alias("c")
    joined = new.join(old, F.col("s.ruc") == F.col("c.ruc"), "full_outer")

    change_type = (
        F.when(F.col("c.ruc").isNull(), "new")
        .when(F.col("s.ruc").isNull(), "removed")
        .when(F.col("s.row_hash") != F.col("c.row_hash"), "changed")
    )
    differs = [F.when(~F.col(f"s.{c}").eqNullSafe(F.col(f"c.{c}")), F.lit(c)) for c in TRACKED]
    changed_columns = F.when(
        change_type == "changed", F.filter(F.array(*differs), lambda name: name.isNotNull())
    )
    return joined.where(change_type.isNotNull()).select(
        F.coalesce("s.ruc", "c.ruc").alias("ruc"),
        *(F.col(f"s.{c}").alias(c) for c in TRACKED),
        change_type.alias("change_type"),
        changed_columns.alias("changed_columns"),
    )


def ensure_table(spark: SparkSession) -> None:
    spark.sql(f"create namespace if not exists {CATALOG}.silver")
    ensure_runs_table(spark)
    if spark.catalog.tableExists(HISTORY):
        return
    columns = ", ".join(f"{c} string" for c in contracts.COLUMNS)
    spark.sql(
        f"create table {HISTORY} ({columns}, valid_from date, "
        f"valid_to date, is_current boolean, change_type string, "
        f"changed_columns array<string>, end_reason string) using iceberg {TABLE_PROPERTIES}"
    )
    spark.sql(f"alter table {HISTORY} write ordered by ruc")


def merge(spark: SparkSession, changes: DataFrame, snapshot_date: date, initial: bool) -> None:
    """Close the current version of changed and removed RUCs, open the new ones.

    Each change appears once with ``merge_key = ruc`` (matches and closes the
    current version, or inserts a new RUC) and, if changed, once more with a
    null key, which never matches and inserts the new version.
    """
    from pyspark.sql import functions as F

    keyed = changes.withColumn("merge_key", F.col("ruc"))
    reopened = changes.where("change_type = 'changed'").withColumn(
        "merge_key", F.lit(None).cast("string")
    )
    keyed.unionByName(reopened).createOrReplaceTempView("silver_changes")

    day = f"date '{snapshot_date.isoformat()}'"
    opened_as = "'initial'" if initial else "s.change_type"
    columns = ", ".join(contracts.COLUMNS)
    values = ", ".join(f"s.{c}" for c in contracts.COLUMNS)
    spark.sql(
        f"""
        merge into {HISTORY} t
        using silver_changes s
        on t.ruc = s.merge_key and t.is_current
        when matched then update set
            valid_to = {day}, is_current = false, end_reason = s.change_type
        when not matched and s.change_type <> 'removed' then insert
            ({columns}, valid_from, valid_to, is_current, change_type,
             changed_columns, end_reason)
            values ({values}, {day}, null, true, {opened_as},
             s.changed_columns, null)
        """
    )


def apply(spark: SparkSession, snapshot_date: date) -> dict[str, int]:
    from pyspark.sql import functions as F

    started = datetime.now(UTC)
    ensure_table(spark)

    last = last_succeeded(spark, JOB)
    if last is not None and snapshot_date < last:
        raise OutOfOrderSnapshot(f"{snapshot_date} is older than {last}, already applied")
    if snapshot_date == last:
        print(f"{snapshot_date}: already applied, nothing to do")
        return {}

    snapshot = spark.table(BRONZE).where(F.col("snapshot_date") == F.lit(snapshot_date))
    # Without this check a missing partition would look like every RUC removed.
    rows_in = snapshot.count()
    if rows_in == 0:
        raise ValueError(f"bronze has no rows for {snapshot_date}")
    bronze_version = spark.sql(
        f"select snapshot_id from {BRONZE}.snapshots order by committed_at desc limit 1"
    ).first()
    current = spark.table(HISTORY).where("is_current")
    initial = last is None

    changes = diff(snapshot, current).cache()
    counts = {
        row["change_type"]: row["n"]
        for row in changes.groupBy("change_type").agg(F.count("*").alias("n")).collect()
    }
    metrics = {
        "rows_in": rows_in,
        "new": counts.get("new", 0),
        "changed": counts.get("changed", 0),
        "removed": counts.get("removed", 0),
    }
    run = {
        "job": JOB,
        "snapshot_date": snapshot_date,
        "input": f"{BRONZE}@{bronze_version['snapshot_id'] if bronze_version else 'empty'}",
        "metrics": metrics,
        "started_at": started,
    }
    try:
        check_removed(metrics["removed"], current.count())
        if counts:
            merge(spark, changes, snapshot_date, initial)
    except SuspiciousSnapshot as error:
        record_run(spark, **run, status="failed", message=str(error), finished_at=datetime.now(UTC))
        raise
    finally:
        changes.unpersist()

    record_run(spark, **run, status="succeeded", finished_at=datetime.now(UTC))
    label = "initial load" if initial else "applied"
    print(
        f"{snapshot_date}: {label}, {metrics['new']:,} new, {metrics['changed']:,} changed, "
        f"{metrics['removed']:,} removed out of {rows_in:,} rows"
    )
    return metrics


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("snapshot_date", type=date.fromisoformat)
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    spark = build_session(f"silver-{args.snapshot_date}", settings)
    try:
        apply(spark, args.snapshot_date)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
