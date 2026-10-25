import pytest

from ruc_lakehouse import contracts
from ruc_lakehouse.jobs.bronze import ContractViolation, check_counts, parse
from tests.fixtures.make_snapshot import BAD_LINES, valid_line


def test_counts_must_reconcile_with_the_landing_marker():
    with pytest.raises(ContractViolation, match="landing marker"):
        check_counts(expected_lines=10, loaded=8, rejected=1)


def test_too_many_rejects_fail_the_load():
    check_counts(expected_lines=10_000, loaded=9_990, rejected=10)
    with pytest.raises(ContractViolation, match="rejected"):
        check_counts(expected_lines=10_000, loaded=9_980, rejected=20)


@pytest.mark.spark
def test_parse_maps_columns_and_flags_broken_lines(spark):
    lines = spark.createDataFrame(
        [(valid_line(1), "a.gz"), (valid_line(3), "a.gz"), *((bad, "b.gz") for bad in BAD_LINES)],
        "value string, source_file string",
    )
    rows = {row["raw_line"]: row for row in parse(lines).collect()}

    person = rows[valid_line(1)]
    assert person["reject_reason"] is None
    assert person["name"] == "PEÑA QUISPE PERSONA 1"
    assert person["ubigeo"] is None  # "-" means empty
    assert person["source_file"] == "a.gz"
    assert set(contracts.COLUMNS) <= set(person.asDict())

    company = rows[valid_line(3)]
    assert company["ubigeo"] == "150101"
    assert company["street_number"] == "3"

    assert rows[BAD_LINES[0]]["reject_reason"] == "field_count"
    assert rows[BAD_LINES[1]]["reject_reason"] == "invalid_ruc"


@pytest.mark.spark
def test_an_escaped_delimiter_stays_inside_the_value(spark):
    lines = spark.createDataFrame([(valid_line(15),)], "value string")
    row = parse(lines).first()
    assert row["reject_reason"] is None
    assert row["name"] == "EMPRESA ÑANDÚ 15 | SERVICIOS S.A.C."
    assert row["ubigeo"] == "150101"


@pytest.mark.spark
def test_an_escaped_backslash_ends_the_value_and_a_lone_one_stays(spark):
    # As in the real file: "S.A.\\|" is a value ending in a backslash, followed by
    # a delimiter; "\ " is a literal backslash that SUNAT did not escape.
    line = r"20123456789|CENTRO S.A.\\|ACTIVO|HABIDO|150101|JR.|LIMA\ NORTE|-|-|-|-|-|-|-|-|"
    row = parse(spark.createDataFrame([(line,)], "value string")).first()
    assert row["reject_reason"] is None
    assert row["name"] == "CENTRO S.A.\\"
    assert row["taxpayer_status"] == "ACTIVO"
    assert row["street_name"] == r"LIMA\ NORTE"


@pytest.mark.spark
def test_a_line_with_an_extra_field_is_rejected(spark):
    lines = spark.createDataFrame([(valid_line(3) + "EXTRA|",)], "value string")
    assert parse(lines).first()["reject_reason"] == "field_count"
