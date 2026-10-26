import json

import pytest

pytest.importorskip("dagster")

from dagster import AssetKey, DagsterInstance, SensorResult, SkipReason, build_sensor_context

from ruc_lakehouse.orchestration.dbt import dbt_project

if not dbt_project.manifest_path.exists():
    pytest.skip("run `dbt parse` first to build the manifest", allow_module_level=True)

from ruc_lakehouse.orchestration.assets import BRONZE, LANDING, SILVER
from ruc_lakehouse.orchestration.definitions import defs
from ruc_lakehouse.orchestration.sensors import new_snapshot_sensor, stored_snapshots


def manifest_line(day: str, stored: bool = True) -> str:
    path = f"/data/raw/snapshot_date={day}/padron_reducido_ruc.zip" if stored else None
    duplicate = None if stored else "2026-10-25"
    return json.dumps({"snapshot_date": day, "path": path, "duplicate_of": duplicate})


def test_definitions_load():
    defs.validate_loadable(defs)
    assert defs.resolve_job_def("ingest_snapshot")
    assert defs.resolve_job_def("build_gold")
    assert defs.resolve_job_def("maintain_tables")


def test_maintenance_runs_weekly_before_the_collector():
    schedule = defs.resolve_schedule_def("weekly_maintenance")
    assert schedule.cron_schedule == "0 4 * * 0"
    assert schedule.execution_timezone == "America/Lima"


def test_history_has_a_freshness_policy():
    spec = defs.resolve_assets_def(SILVER).specs_by_key[SILVER]
    assert spec.freshness_policy is not None


def test_gold_hangs_off_silver_and_layers_chain():
    graph = defs.resolve_asset_graph()
    assert graph.get(BRONZE).parent_keys == {LANDING}
    assert graph.get(SILVER).parent_keys == {BRONZE}
    staging = AssetKey(["staging", "stg_taxpayer_versions"])
    assert SILVER in graph.get(staging).parent_keys


def test_stored_snapshots_skip_identical_days_and_sort():
    lines = [manifest_line("2026-10-27"), manifest_line("2026-10-26", stored=False), ""]
    lines.append(manifest_line("2026-10-25"))
    assert stored_snapshots(lines) == ["2026-10-25", "2026-10-27"]


def test_sensor_requests_only_new_snapshots_oldest_first(tmp_path, monkeypatch):
    lines = [manifest_line(day) for day in ("2026-10-26", "2026-10-25")]
    (tmp_path / "manifest.jsonl").write_text("\n".join(lines) + "\n")
    monkeypatch.setenv("SNAPSHOTS_DIR", str(tmp_path))

    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("snapshots", ["2026-10-25"])
        result = new_snapshot_sensor(build_sensor_context(instance=instance))

    assert isinstance(result, SensorResult)
    assert [request.partition_key for request in result.run_requests] == ["2026-10-26"]
    assert result.dynamic_partitions_requests[0].partition_keys == ["2026-10-26"]


def test_sensor_skips_without_a_manifest(tmp_path, monkeypatch):
    monkeypatch.setenv("SNAPSHOTS_DIR", str(tmp_path))
    with DagsterInstance.ephemeral() as instance:
        result = new_snapshot_sensor(build_sensor_context(instance=instance))
    assert isinstance(result, SkipReason)
