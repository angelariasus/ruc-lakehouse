from datetime import date, datetime

from ruc_lakehouse.jobs.maintenance import bronze_cutoff, call, options, timestamp


def test_bronze_keeps_the_last_days_before_silver():
    assert bronze_cutoff(date(2026, 11, 20), 14) == date(2026, 11, 6)


def test_bronze_is_kept_while_silver_is_empty():
    assert bronze_cutoff(None, 14) is None


def test_procedure_call_names_its_arguments():
    sql = call(
        "expire_snapshots",
        "lakehouse.silver.taxpayers_history",
        older_than=timestamp(datetime(2026, 11, 1, 3, 4, 5)),
    )
    assert sql == (
        "call lakehouse.system.expire_snapshots(table => 'lakehouse.silver.taxpayers_history', "
        "older_than => timestamp '2026-11-01 03:04:05')"
    )


def test_options_become_a_spark_map():
    assert options({"a": "1", "b": "x"}) == "map('a', '1', 'b', 'x')"
