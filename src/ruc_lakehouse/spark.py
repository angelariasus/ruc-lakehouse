"""Spark sessions wired to the Iceberg catalog of the active profile."""

from __future__ import annotations

import os
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING

from ruc_lakehouse.config import Settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

CATALOG = "lakehouse"
ICEBERG_VERSION = "1.12.0"
SPARK_MINOR = "4.1"
SCALA_VERSION = "2.13"
# Hadoop S3A reads raw files (staging). It must match the Hadoop client inside
# PySpark, and it reuses the AWS SDK of the Iceberg bundle plus the two SDK
# modules that bundle leaves out, instead of the full 600 MB SDK.
HADOOP_VERSION = "3.4.2"
AWS_SDK_VERSION = "2.54.17"
PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR}_{SCALA_VERSION}:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",
    f"software.amazon.awssdk:s3-transfer-manager:{AWS_SDK_VERSION}",
    f"software.amazon.awssdk:apache-client:{AWS_SDK_VERSION}",
)
# Where the EMR runtime (EMR Serverless and its job image) keeps Iceberg.
EMR_ICEBERG_DIR = Path("/usr/share/aws/iceberg/lib")


def iceberg_conf(settings: Settings) -> dict[str, str]:
    """Spark properties that register the Iceberg catalog as ``lakehouse``."""
    prefix = f"spark.sql.catalog.{CATALOG}"
    conf = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        "spark.sql.defaultCatalog": CATALOG,
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": settings.catalog_uri,
        f"{prefix}.warehouse": settings.warehouse,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.client.region": settings.region,
    }
    if settings.profile == "local":
        conf |= {
            # Use our own keys for data files, as an IAM role would on AWS.
            f"{prefix}.header.X-Iceberg-Access-Delegation": "client-managed",
            f"{prefix}.s3.endpoint": settings.s3_endpoint or "",
            f"{prefix}.s3.path-style-access": "true",
            f"{prefix}.s3.access-key-id": settings.s3_access_key or "",
            f"{prefix}.s3.secret-access-key": settings.s3_secret_key or "",
        }
    else:
        # Glue's REST endpoint authenticates every request with SigV4.
        conf |= {
            f"{prefix}.rest.sigv4-enabled": "true",
            f"{prefix}.rest.signing-name": "glue",
            f"{prefix}.rest.signing-region": settings.region,
        }
    return conf


def s3a_conf(settings: Settings) -> dict[str, str]:
    """Spark properties for reading raw files through Hadoop S3A."""
    prefix = "spark.hadoop.fs.s3a"
    conf = {f"{prefix}.endpoint.region": settings.region}
    if settings.profile == "local":
        endpoint = settings.s3_endpoint or ""
        conf |= {
            f"{prefix}.endpoint": endpoint,
            f"{prefix}.path.style.access": "true",
            f"{prefix}.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
            f"{prefix}.access.key": settings.s3_access_key or "",
            f"{prefix}.secret.key": settings.s3_secret_key or "",
        }
    return conf


def jars_bundled(emr_iceberg_dir: Path = EMR_ICEBERG_DIR) -> bool:
    """True when the runtime already has the extra jars on Spark's classpath.

    The project image ships them inside PySpark; the EMR runtime ships its own
    Iceberg, S3A and AWS SDK builds.
    """
    if emr_iceberg_dir.is_dir():
        return True
    spec = find_spec("pyspark")
    if spec is None or spec.origin is None:
        return False
    jars = Path(spec.origin).parent / "jars"
    return any(jars.glob("iceberg-spark-runtime-*.jar"))


def build_session(app_name: str, settings: Settings | None = None) -> SparkSession:
    from pyspark.sql import SparkSession

    settings = settings or Settings.from_env()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    if not jars_bundled():
        builder = builder.config("spark.jars.packages", ",".join(PACKAGES))
    for key, value in (iceberg_conf(settings) | s3a_conf(settings)).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
