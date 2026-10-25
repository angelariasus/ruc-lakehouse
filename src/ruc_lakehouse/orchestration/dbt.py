"""The dbt project as Dagster assets; dbt tests become asset checks."""

import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(
    os.environ.get("DBT_PROJECT_DIR", Path(__file__).resolve().parents[3] / "dbt")
)

# The manifest is built with `dbt parse` (in the image, at build time).
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
