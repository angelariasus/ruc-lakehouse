"""Weekly table maintenance, as an asset so each pass keeps its metrics.

The pass runs early on Sunday, Lima time, hours before the collector's 09:00
download. With one run at a time it never overlaps an ingestion.
"""

import sys

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DefaultScheduleStatus,
    MaterializeResult,
    PipesSubprocessClient,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

MAINTENANCE = AssetKey(["ops", "table_maintenance"])


@asset(
    key=MAINTENANCE,
    group_name="maintenance",
    kinds={"spark", "iceberg"},
    description=(
        "Bronze retention, compaction of silver and the run log, snapshot expiry "
        "and orphan file cleanup on every table."
    ),
)
def table_maintenance(
    context: AssetExecutionContext, pipes: PipesSubprocessClient
) -> MaterializeResult:
    command = [sys.executable, "-m", "ruc_lakehouse.jobs.maintenance"]
    return pipes.run(command=command, context=context).get_materialize_result()


maintain_tables = define_asset_job(
    "maintain_tables", selection=[table_maintenance], description="Weekly table maintenance."
)

weekly_maintenance = ScheduleDefinition(
    name="weekly_maintenance",
    job=maintain_tables,
    cron_schedule="0 4 * * 0",
    execution_timezone="America/Lima",
    default_status=DefaultScheduleStatus.RUNNING,
)
