"""Dagster assets: one per lakehouse layer, partitioned by snapshot date.

The partitions are dynamic: one per snapshot that exists, added by the sensor
when the collector stores a new file. SUNAT does not publish every day, so a
calendar of daily partitions would list days with nothing to load.

Each asset runs its job as a separate process through Dagster Pipes; the job
reports its metrics back as materialization metadata. Locally the process
runs next to the Dagster daemon. On AWS, the same command would run on EMR
Serverless by swapping the Pipes client (``PipesEMRServerlessClient``).
"""

import sys
from datetime import timedelta

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DynamicPartitionsDefinition,
    FreshnessPolicy,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
)

snapshots = DynamicPartitionsDefinition(name="snapshots")

LANDING = AssetKey(["landing", "padron_ruc"])
BRONZE = AssetKey(["bronze", "padron_ruc"])
# Same key dbt gives its source, so gold models hang off this asset.
SILVER = AssetKey(["silver", "taxpayers_history"])

# SUNAT publishes every day, but a day whose file is identical to the previous
# one loads nothing: warn after a day and a half, fail after two days.
HISTORY_FRESHNESS = FreshnessPolicy.time_window(
    fail_window=timedelta(hours=48), warn_window=timedelta(hours=30)
)


def run_job(
    context: AssetExecutionContext, pipes: PipesSubprocessClient, module: str
) -> MaterializeResult:
    command = [sys.executable, "-m", module, context.partition_key]
    return pipes.run(command=command, context=context).get_materialize_result()


@asset(
    key=LANDING,
    partitions_def=snapshots,
    group_name="ingestion",
    kinds={"python", "s3"},
    description="The zip as published plus UTF-8 gzip parts for Spark.",
)
def landing(context: AssetExecutionContext, pipes: PipesSubprocessClient) -> MaterializeResult:
    return run_job(context, pipes, "ruc_lakehouse.ingest.landing")


@asset(
    key=BRONZE,
    deps=[LANDING],
    partitions_def=snapshots,
    group_name="ingestion",
    kinds={"spark", "iceberg"},
    description="Valid lines of the snapshot; broken ones go to the rejects table.",
)
def bronze(context: AssetExecutionContext, pipes: PipesSubprocessClient) -> MaterializeResult:
    return run_job(context, pipes, "ruc_lakehouse.jobs.bronze")


@asset(
    key=SILVER,
    deps=[BRONZE],
    partitions_def=snapshots,
    group_name="history",
    kinds={"spark", "iceberg"},
    freshness_policy=HISTORY_FRESHNESS,
    description="SCD type 2 history of every RUC; dates must be applied in order.",
)
def silver(context: AssetExecutionContext, pipes: PipesSubprocessClient) -> MaterializeResult:
    return run_job(context, pipes, "ruc_lakehouse.jobs.silver")
