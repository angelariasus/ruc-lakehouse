"""Object storage layout and client.

s3://<bucket>/landing/padron_ruc/snapshot_date=YYYY-MM-DD/padron_reducido_ruc.zip
    the file exactly as SUNAT published it (immutable)
s3://<bucket>/staging/padron_ruc/snapshot_date=YYYY-MM-DD/part-NNNNN.txt.gz
    the same lines in UTF-8 gzip parts, so Spark can read them in parallel
s3://<bucket>/warehouse/...
    Iceberg tables, managed by the catalog
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ruc_lakehouse.config import Settings

SOURCE_FILE = "padron_reducido_ruc.zip"
MARKER = "_SUCCESS.json"


def landing_key(snapshot_date: date) -> str:
    return f"landing/padron_ruc/snapshot_date={snapshot_date.isoformat()}/{SOURCE_FILE}"


def staging_prefix(snapshot_date: date) -> str:
    return f"staging/padron_ruc/snapshot_date={snapshot_date.isoformat()}/"


def spark_uri(settings: Settings, key: str) -> str:
    """URI Spark reads raw files from (Hadoop S3A, also available on EMR)."""
    return f"s3a://{settings.bucket}/{key}"


def s3_client(settings: Settings) -> Any:
    import boto3
    from botocore.config import Config

    if settings.profile == "local":
        return boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.region,
            config=Config(s3={"addressing_style": "path"}),
        )
    return boto3.client("s3", region_name=settings.region)
