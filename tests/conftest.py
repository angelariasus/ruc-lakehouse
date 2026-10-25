import os
import shutil
import sys

import pytest


@pytest.fixture(scope="session")
def spark():
    """Plain local Spark (no Iceberg, no S3), for testing transformations."""
    if shutil.which("java") is None:
        pytest.skip("Java is not installed")
    from pyspark.sql import SparkSession

    # Workers must run the test interpreter (on Windows Spark may not find one).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
