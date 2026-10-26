"""Dagster entry point (``workspace.yaml`` loads this module)."""

from dagster import Definitions, PipesSubprocessClient
from dagster_dbt import DbtCliResource

from ruc_lakehouse.orchestration.assets import bronze, landing, silver
from ruc_lakehouse.orchestration.dbt import dbt_models, dbt_project
from ruc_lakehouse.orchestration.maintenance import (
    maintain_tables,
    table_maintenance,
    weekly_maintenance,
)
from ruc_lakehouse.orchestration.sensors import (
    build_gold,
    gold_after_ingestion,
    ingest_snapshot,
    new_snapshot_sensor,
)

defs = Definitions(
    assets=[landing, bronze, silver, dbt_models, table_maintenance],
    jobs=[ingest_snapshot, build_gold, maintain_tables],
    sensors=[new_snapshot_sensor, gold_after_ingestion],
    schedules=[weekly_maintenance],
    resources={
        "pipes": PipesSubprocessClient(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
