import pytest

from ruc_lakehouse import contracts
from ruc_lakehouse.jobs.silver import (
    TRACKED,
    SuspiciousSnapshot,
    check_removed,
    diff,
    with_row_hash,
)

SCHEMA = ", ".join(f"{c} string" for c in contracts.COLUMNS)


def taxpayer(ruc: str, **values: str | None) -> tuple:
    base = {c: None for c in contracts.COLUMNS}
    base |= {"ruc": ruc, "name": f"EMPRESA {ruc}", "taxpayer_status": "ACTIVO"}
    base |= values
    return tuple(base[c] for c in contracts.COLUMNS)


def test_a_snapshot_that_empties_the_registry_is_refused():
    check_removed(removed=10, current=10_000)
    with pytest.raises(SuspiciousSnapshot):
        check_removed(removed=500, current=10_000)
    check_removed(removed=0, current=0)


@pytest.mark.spark
def test_diff_finds_new_changed_and_removed_rucs(spark):
    yesterday = spark.createDataFrame(
        [
            taxpayer("20000000001"),
            taxpayer("20000000002"),
            taxpayer("20000000003"),
            taxpayer("20000000004", ubigeo="150101"),
        ],
        SCHEMA,
    )
    today = spark.createDataFrame(
        [
            taxpayer("20000000001"),  # unchanged
            taxpayer("20000000002", taxpayer_status="BAJA DE OFICIO"),  # changed
            taxpayer("20000000004", ubigeo=None),  # value removed: also a change
            taxpayer("20000000005"),  # new
        ],
        SCHEMA,
    )
    rows = {row["ruc"]: row for row in diff(today, yesterday).collect()}

    assert set(rows) == {"20000000002", "20000000003", "20000000004", "20000000005"}
    assert rows["20000000002"]["change_type"] == "changed"
    assert rows["20000000002"]["changed_columns"] == ["taxpayer_status"]
    assert rows["20000000002"]["taxpayer_status"] == "BAJA DE OFICIO"
    assert rows["20000000003"]["change_type"] == "removed"
    assert rows["20000000004"]["changed_columns"] == ["ubigeo"]
    assert rows["20000000005"]["change_type"] == "new"
    assert rows["20000000005"]["changed_columns"] is None


@pytest.mark.spark
def test_row_hash_tells_null_from_empty_and_shifted_values(spark):
    rows = [
        taxpayer("20000000001", street_name="AB", zone_code="C"),
        taxpayer("20000000002", street_name="A", zone_code="BC"),
        taxpayer("20000000003", street_name=None),
        taxpayer("20000000004", street_name=""),
    ]
    hashes = [r["row_hash"] for r in with_row_hash(spark.createDataFrame(rows, SCHEMA)).collect()]
    assert len(set(hashes)) == 4
    assert "ruc" not in TRACKED
