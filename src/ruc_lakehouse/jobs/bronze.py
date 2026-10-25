"""Load one landed snapshot into the bronze Iceberg tables.

    python -m ruc_lakehouse.jobs.bronze 2026-10-25

- ``bronze.padron_ruc``: one row per valid line, partitioned by snapshot date
  and sorted by RUC. Values are kept as published, except that SUNAT's ``-``
  becomes null.
- ``bronze.padron_ruc_rejects``: lines that break the contract, with the reason.
- ``ops.ingestion_runs``: one row per run with lines read, loaded and rejected.

Before writing, the job reconciles its counts with the landing marker and
checks the reject ratio, so a broken file never reaches the table. Running it
again replaces the snapshot's partition, so reruns are safe.
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from ruc_lakehouse import contracts
from ruc_lakehouse.config import Settings
from ruc_lakehouse.ingest.landing import read_marker
from ruc_lakehouse.spark import CATALOG, build_session
from ruc_lakehouse.storage import MARKER, s3_client, spark_uri, staging_prefix

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

BRONZE = f"{CATALOG}.bronze.padron_ruc"
REJECTS = f"{CATALOG}.bronze.padron_ruc_rejects"
RUNS = f"{CATALOG}.ops.ingestion_runs"

TABLE_PROPERTIES = (
    "tblproperties ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')"
)


class ContractViolation(RuntimeError):
    """The snapshot cannot be loaded: counts do not reconcile or too many rejects."""


_ESC = re.escape(contracts.SOURCE_ESCAPE)
_DELIM = re.escape(contracts.SOURCE_DELIMITER)
# One value: characters other than the escape and the delimiter, or an escaped
# character. Possessive quantifiers keep Java's regex engine from backtracking.
VALUE = f"(?:[^{_ESC}{_DELIM}]++|{_ESC}.)*+"
# Every value, the last one included, is followed by a delimiter.
LINE_PATTERN = f"^(?:{VALUE}{_DELIM}){{{len(contracts.COLUMNS)}}}$"
VALUE_PATTERN = f"({VALUE}){_DELIM}"
# Only an escaped escape or delimiter is unescaped; any other backslash in the
# file is a literal one and stays.
UNESCAPE_PATTERN = f"{_ESC}([{_ESC}{_DELIM}])"


def clean(value: Column) -> Column:
    from pyspark.sql import functions as F

    trimmed = F.trim(F.regexp_replace(value, UNESCAPE_PATTERN, "$1"))
    return F.when(trimmed.isin(contracts.NULL_TOKEN, ""), None).otherwise(trimmed)


def parse(lines: DataFrame) -> DataFrame:
    """Split raw lines into the contract columns plus ``reject_reason``.

    ``reject_reason`` is null for a valid line. Expects the ``value`` column of
    Spark's text source; any other column is kept as is.
    """
    from pyspark.sql import functions as F

    fields = F.regexp_extract_all("value", F.lit(VALUE_PATTERN), 1)
    malformed = ~F.col("value").rlike(LINE_PATTERN)
    reason = F.when(malformed, "field_count").when(
        ~F.trim(F.get(fields, 0)).rlike(contracts.RUC_PATTERN), "invalid_ruc"
    )
    return lines.select(
        *(clean(F.get(fields, i)).alias(name) for i, name in enumerate(contracts.COLUMNS)),
        reason.alias("reject_reason"),
        F.col("value").alias("raw_line"),
        *(column for column in lines.columns if column != "value"),
    )


def ensure_tables(spark: SparkSession) -> None:
    columns = ", ".join(f"{name} string" for name in contracts.COLUMNS)
    for namespace in ("bronze", "ops"):
        spark.sql(f"create namespace if not exists {CATALOG}.{namespace}")

    if not spark.catalog.tableExists(BRONZE):
        spark.sql(
            f"create table {BRONZE} (snapshot_date date, {columns}, source_file string, "
            f"ingested_at timestamp) using iceberg partitioned by (snapshot_date) "
            f"{TABLE_PROPERTIES}"
        )
        # Sorted files let later jobs skip most of them when joining on RUC.
        spark.sql(f"alter table {BRONZE} write ordered by ruc")

    spark.sql(
        f"create table if not exists {REJECTS} (snapshot_date date, reject_reason string, "
        f"raw_line string, source_file string, ingested_at timestamp) using iceberg "
        f"partitioned by (snapshot_date) {TABLE_PROPERTIES}"
    )
    spark.sql(
        f"create table if not exists {RUNS} (run_id string, job string, snapshot_date date, "
        f"source_sha256 string, lines_read bigint, rows_loaded bigint, rows_rejected bigint, "
        f"status string, message string, started_at timestamp, finished_at timestamp) "
        f"using iceberg {TABLE_PROPERTIES}"
    )


def check_counts(expected_lines: int, loaded: int, rejected: int) -> None:
    read = loaded + rejected
    if read != expected_lines:
        raise ContractViolation(f"read {read:,} lines, landing marker says {expected_lines:,}")
    if read and rejected / read > contracts.MAX_REJECT_RATIO:
        raise ContractViolation(
            f"{rejected:,} of {read:,} lines rejected, above {contracts.MAX_REJECT_RATIO:.1%}"
        )


def record_run(spark: SparkSession, **values: object) -> None:
    values = {"run_id": str(uuid.uuid4()), "job": "bronze", **values}
    schema = spark.table(RUNS).schema
    # createDataFrame matches tuples by position, so follow the table's order.
    row = tuple(values[field.name] for field in schema.fields)
    spark.createDataFrame([row], schema=schema).writeTo(RUNS).append()


def load(spark: SparkSession, settings: Settings, snapshot_date: date) -> dict[str, int]:
    from pyspark.sql import functions as F

    started = datetime.now(UTC)
    prefix = staging_prefix(snapshot_date)
    marker = read_marker(s3_client(settings), settings.bucket, prefix + MARKER)
    if marker is None:
        raise ContractViolation(f"{snapshot_date} is not landed: no {prefix}{MARKER}")

    ensure_tables(spark)
    # Spark skips files starting with "_", so the marker is not read as data.
    # _metadata exists only on the file scan, so take the file name before parsing.
    lines = spark.read.text(spark_uri(settings, prefix)).select(
        "value", F.col("_metadata.file_name").alias("source_file")
    )
    parsed = parse(lines).select(
        F.lit(snapshot_date).alias("snapshot_date"),
        "*",
        F.current_timestamp().alias("ingested_at"),
    )

    totals = parsed.agg(
        F.count("*").alias("lines"),
        F.count_if(F.col("reject_reason").isNull()).alias("valid"),
    ).first()
    loaded = totals["valid"] if totals else 0
    rejected = (totals["lines"] if totals else 0) - loaded
    run = {
        "snapshot_date": snapshot_date,
        "source_sha256": marker["sha256"],
        "lines_read": loaded + rejected,
        "rows_loaded": loaded,
        "rows_rejected": rejected,
        "started_at": started,
    }
    try:
        check_counts(marker["lines"], loaded, rejected)
    except ContractViolation as error:
        record_run(spark, **run, status="failed", message=str(error), finished_at=datetime.now(UTC))
        raise

    bronze_columns = spark.table(BRONZE).columns
    parsed.where("reject_reason is null").select(*bronze_columns).writeTo(
        BRONZE
    ).overwritePartitions()
    parsed.where("reject_reason is not null").select(
        "snapshot_date", "reject_reason", "raw_line", "source_file", "ingested_at"
    ).writeTo(REJECTS).overwritePartitions()

    record_run(spark, **run, status="succeeded", message=None, finished_at=datetime.now(UTC))
    print(f"{snapshot_date}: {loaded:,} rows loaded, {rejected:,} rejected")
    return {"loaded": loaded, "rejected": rejected}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("snapshot_date", type=date.fromisoformat)
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    spark = build_session(f"bronze-{args.snapshot_date}", settings)
    try:
        load(spark, settings, args.snapshot_date)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
