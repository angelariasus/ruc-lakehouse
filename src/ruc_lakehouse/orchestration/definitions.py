"""Dagster entry point (``workspace.yaml`` loads this module)."""

from dagster import Definitions, PipesSubprocessClient
from dagster_dbt import DbtCliResource

from ruc_lakehouse.orchestration.assets import bronze, landing, silver
from ruc_lakehouse.orchestration.dbt import dbt_models, dbt_project
from ruc_lakehouse.orchestration.sensors import (
    build_gold,
    gold_after_ingestion,
    ingest_snapshot,
    new_snapshot_sensor,
)

defs = Definitions(
    assets=[landing, bronze, silver, dbt_models],
    jobs=[ingest_snapshot, build_gold],
    sensors=[new_snapshot_sensor, gold_after_ingestion],
    resources={
        "pipes": PipesSubprocessClient(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
