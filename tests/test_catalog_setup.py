from ruc_lakehouse.config import Settings
from ruc_lakehouse.setup.catalog import management_url, warehouse_body


def settings() -> Settings:
    return Settings(
        profile="local",
        bucket="lake",
        region="us-east-1",
        catalog_uri="http://catalog:8181/catalog/",
        warehouse="ruc",
        s3_endpoint="http://storage:9000",
        s3_access_key="key",
        s3_secret_key="secret",
    )


def test_management_url_is_derived_from_the_catalog_uri():
    assert management_url(settings()) == "http://catalog:8181/management/v1"


def test_warehouse_lives_in_the_lake_bucket_without_credential_vending():
    body = warehouse_body(settings())
    profile = body["storage-profile"]
    assert body["warehouse-name"] == "ruc"
    assert profile["bucket"] == "lake"
    assert profile["sts-enabled"] is False
    assert body["storage-credential"]["access-key-id"] == "key"
