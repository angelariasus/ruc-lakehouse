"""Run log shared by every job: ``ops.job_runs``.

One row per run, with what it read (``input``), what it did (``metrics``) and
how it ended. Later runs use it as a watermark, e.g. the last snapshot date
silver has applied.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import date, datetime
from typing import TYPE_CHECKING

from ruc_lakehouse.spark import CATALOG

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

RUNS = f"{CATALOG}.ops.job_runs"


def ensure_runs_table(spark: SparkSession) -> None:
    spark.sql(f"create namespace if not exists {CATALOG}.ops")
    spark.sql(
        f"create table if not exists {RUNS} (run_id string, job string, snapshot_date date, "
        f"status string, input string, metrics map<string, bigint>, message string, "
        f"started_at timestamp, finished_at timestamp) using iceberg "
        f"tblproperties ('format-version' = '2')"
    )


def record_run(
    spark: SparkSession,
    *,
    job: str,
    snapshot_date: date,
    status: str,
    input: str,
    metrics: Mapping[str, int],
    started_at: datetime,
    finished_at: datetime,
    message: str | None = None,
) -> None:
    values = {
        "run_id": str(uuid.uuid4()),
        "job": job,
        "snapshot_date": snapshot_date,
        "status": status,
        "input": input,
        "metrics": dict(metrics),
        "message": message,
        "started_at": started_at,
        "finished_at": finished_at,
    }
    schema = spark.table(RUNS).schema
    # createDataFrame matches tuples by position, so follow the table's order.
    row = tuple(values[field.name] for field in schema.fields)
    spark.createDataFrame([row], schema=schema).writeTo(RUNS).append()


def last_succeeded(spark: SparkSession, job: str) -> date | None:
    """Latest snapshot date the job has applied successfully."""
    row = spark.sql(
        f"select max(snapshot_date) as last from {RUNS} where job = '{job}' "
        f"and status = 'succeeded'"
    ).first()
    return row["last"] if row else None
