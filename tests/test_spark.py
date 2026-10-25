from ruc_lakehouse.config import Settings
from ruc_lakehouse.spark import CATALOG, ICEBERG_PACKAGES, SPARK_MINOR, iceberg_conf

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


def test_iceberg_runtime_matches_the_installed_spark_minor():
    import pyspark

    assert pyspark.__version__.startswith(SPARK_MINOR + ".")
    assert f"iceberg-spark-runtime-{SPARK_MINOR}_" in ICEBERG_PACKAGES[0]
