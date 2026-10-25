"""End-to-end check of the stack: Spark writes an Iceberg table, reads it back.

    python -m ruc_lakehouse.jobs.smoke

Trino can then read the same table (``lakehouse.smoke.ping``), which proves
that storage, catalog and both engines agree.
"""

from __future__ import annotations

import sys

from ruc_lakehouse.spark import CATALOG, build_session

TABLE = f"{CATALOG}.smoke.ping"


def main() -> int:
    spark = build_session("smoke")
    spark.sql(f"create namespace if not exists {CATALOG}.smoke")
    spark.sql(f"create table if not exists {TABLE} (id bigint, at timestamp) using iceberg")
    spark.sql(
        f"insert into {TABLE} select coalesce(max(id), 0) + 1, current_timestamp() from {TABLE}"
    )
    rows = spark.table(TABLE).count()
    location = spark.sql(f"describe table extended {TABLE}").where("col_name = 'Location'").first()
    print(f"{TABLE}: {rows} row(s) at {location['data_type'] if location else '?'}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
