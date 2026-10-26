import re
from pathlib import Path

from ruc_lakehouse import spark as spark_module
from ruc_lakehouse.config import Settings
from ruc_lakehouse.spark import CATALOG, PACKAGES, SPARK_MINOR, iceberg_conf, s3a_conf

PREFIX = f"spark.sql.catalog.{CATALOG}"


def local_settings() -> Settings:
    return Settings(
        profile="local",
        bucket="lake",
        region="us-east-1",
        catalog_uri="http://catalog:8181/catalog",
        warehouse="ruc",
        s3_endpoint="http://storage:9000",
        s3_access_key="key",
        s3_secret_key="secret",
    )


def test_local_conf_points_s3_file_io_at_the_local_store():
    conf = iceberg_conf(local_settings())
    assert conf["spark.sql.defaultCatalog"] == CATALOG
    assert conf[f"{PREFIX}.type"] == "rest"
    assert conf[f"{PREFIX}.s3.endpoint"] == "http://storage:9000"
    assert conf[f"{PREFIX}.s3.path-style-access"] == "true"
    assert f"{PREFIX}.rest.sigv4-enabled" not in conf


def test_aws_conf_signs_catalog_requests_and_has_no_keys():
    settings = Settings(
        profile="aws",
        bucket="lake",
        region="sa-east-1",
        catalog_uri="https://glue.sa-east-1.amazonaws.com/iceberg",
        warehouse="123456789012",
    )
    conf = iceberg_conf(settings)
    assert conf[f"{PREFIX}.rest.sigv4-enabled"] == "true"
    assert conf[f"{PREFIX}.rest.signing-name"] == "glue"
    assert not any(key.startswith(f"{PREFIX}.s3.") for key in conf)


def test_local_s3a_conf_uses_path_style_and_plain_http():
    conf = s3a_conf(local_settings())
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://storage:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"


def test_jars_match_the_installed_spark_and_hadoop():
    import pyspark

    assert pyspark.__version__.startswith(SPARK_MINOR + ".")
    assert f"iceberg-spark-runtime-{SPARK_MINOR}_" in PACKAGES[0]
    jars = Path(pyspark.__file__).parent / "jars"
    hadoop = f"hadoop-client-api-{spark_module.HADOOP_VERSION}.jar"
    assert (jars / hadoop).exists(), f"PySpark no longer ships {hadoop}"


def test_the_emr_runtime_counts_as_bundled(tmp_path):
    assert spark_module.jars_bundled(emr_iceberg_dir=tmp_path)


def test_dockerfile_fetches_the_same_jar_versions():
    dockerfile = (Path(__file__).parents[1] / "docker" / "app" / "Dockerfile").read_text()
    args = dict(re.findall(r"^ARG (\w+)=(\S+)$", dockerfile, flags=re.MULTILINE))
    for name in ("ICEBERG_VERSION", "SPARK_MINOR", "SCALA_VERSION", "HADOOP_VERSION"):
        assert args[name] == getattr(spark_module, name), name
    assert args["AWS_SDK_VERSION"] == spark_module.AWS_SDK_VERSION
