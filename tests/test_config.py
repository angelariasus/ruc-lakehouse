import pytest

from ruc_lakehouse.config import Settings, SettingsError

LOCAL = {
    "LAKE_BUCKET": "lake",
    "CATALOG_URI": "http://catalog:8181/catalog",
    "CATALOG_WAREHOUSE": "ruc",
    "S3_ENDPOINT": "http://storage:9000",
    "S3_ACCESS_KEY": "key",
    "S3_SECRET_KEY": "secret",
}


def test_local_is_the_default_profile():
    settings = Settings.from_env(LOCAL)
    assert settings.profile == "local"
    assert settings.region == "us-east-1"
    assert settings.lake_root == "s3://lake"


@pytest.mark.parametrize("missing", ["S3_ENDPOINT", "S3_ACCESS_KEY", "CATALOG_URI"])
def test_local_requires_storage_and_catalog(missing):
    env = {**LOCAL, missing: "  "}
    with pytest.raises(SettingsError, match=missing):
        Settings.from_env(env)


def test_aws_uses_glue_rest_endpoint_and_no_static_keys():
    env = {"LAKE_PROFILE": "aws", "LAKE_BUCKET": "lake", "CATALOG_WAREHOUSE": "123456789012"}
    settings = Settings.from_env({**env, "AWS_REGION": "sa-east-1"})
    assert settings.catalog_uri == "https://glue.sa-east-1.amazonaws.com/iceberg"
    assert settings.s3_endpoint is None
    assert settings.s3_access_key is None


def test_aws_ignores_local_keys():
    settings = Settings.from_env({**LOCAL, "LAKE_PROFILE": "aws"})
    assert settings.s3_access_key is None


def test_unknown_profile_is_rejected():
    with pytest.raises(SettingsError, match="LAKE_PROFILE"):
        Settings.from_env({**LOCAL, "LAKE_PROFILE": "gcp"})
