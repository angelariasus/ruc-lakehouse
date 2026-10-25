"""Sensors: load each new snapshot, then rebuild gold.

- ``new_snapshot_sensor`` reads the collector's ``manifest.jsonl``, adds a
  partition for every stored snapshot Dagster does not know yet and requests
  its ingestion, oldest first, since silver applies dates in order.
- ``gold_after_ingestion`` rebuilds the dbt models after each successful
  ingestion run.

With one run at a time (see ``dagster.yaml``), runs execute in the order they
were requested.
"""

import json
import os
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunStatusSensorContext,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    define_asset_job,
    run_status_sensor,
    sensor,
)

from ruc_lakehouse.orchestration.assets import bronze, landing, silver, snapshots
from ruc_lakehouse.orchestration.dbt import dbt_models

ingest_snapshot = define_asset_job(
    "ingest_snapshot",
    selection=[landing, bronze, silver],
    partitions_def=snapshots,
    description="Landing, bronze and silver for one snapshot.",
)
build_gold = define_asset_job(
    "build_gold", selection=[dbt_models], description="dbt build of the gold layer."
)


def stored_snapshots(manifest_lines: Iterable[str]) -> list[str]:
    """Dates of the snapshots the collector stored as files, oldest first.

    A day whose file was identical to the previous one has no file (the
    collector keeps only its manifest line) and brings no changes.
    """
    dates = set()
    for line in manifest_lines:
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("path"):
            dates.add(date.fromisoformat(record["snapshot_date"]).isoformat())
    return sorted(dates)


def snapshots_dir() -> Path:
    return Path(os.environ.get("SNAPSHOTS_DIR", "/snapshots"))


@sensor(
    job=ingest_snapshot,
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
)
def new_snapshot_sensor(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    manifest = snapshots_dir() / "manifest.jsonl"
    if not manifest.exists():
        return SkipReason(f"no collector manifest at {manifest}")

    known = set(context.instance.get_dynamic_partitions(snapshots.name or "snapshots"))
    new = [d for d in stored_snapshots(manifest.read_text().splitlines()) if d not in known]
    if not new:
        return SkipReason("no new snapshots")
    return SensorResult(
        run_requests=[RunRequest(partition_key=d, run_key=d) for d in new],
        dynamic_partitions_requests=[snapshots.build_add_request(new)],
    )


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[ingest_snapshot],
    request_job=build_gold,
    default_status=DefaultSensorStatus.RUNNING,
)
def gold_after_ingestion(context: RunStatusSensorContext) -> RunRequest:
    return RunRequest(run_key=context.dagster_run.run_id)
