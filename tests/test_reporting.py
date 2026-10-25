from ruc_lakehouse.reporting import materialization_reporter


def test_reporting_is_a_no_op_outside_dagster(monkeypatch):
    monkeypatch.delenv("DAGSTER_PIPES_CONTEXT", raising=False)
    monkeypatch.delenv("DAGSTER_PIPES_MESSAGES", raising=False)
    with materialization_reporter() as report:
        assert report({"rows": 1}) is None
