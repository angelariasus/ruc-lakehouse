import gzip
import json
import zipfile
from datetime import date

import boto3
import pytest
from moto import mock_aws

from ruc_lakehouse import contracts
from ruc_lakehouse.config import Settings
from ruc_lakehouse.ingest.landing import SchemaDriftError, land
from ruc_lakehouse.storage import MARKER, landing_key, staging_prefix
from tests.fixtures.make_snapshot import BAD_LINES, make_snapshot

DAY = date(2026, 1, 15)
SETTINGS = Settings(profile="aws", bucket="lake", region="us-east-1", catalog_uri="", warehouse="")


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="lake")
        yield client


def keys(s3, prefix):
    listing = s3.list_objects_v2(Bucket="lake", Prefix=prefix)
    return sorted(item["Key"] for item in listing.get("Contents", []))


def test_land_keeps_the_zip_and_rechunks_utf8_parts(s3, tmp_path):
    make_snapshot(tmp_path, DAY, valid_rows=5)
    marker = land(SETTINGS, tmp_path, DAY, s3=s3, lines_per_part=3)

    assert marker["lines"] == 5 + len(BAD_LINES)
    assert marker["parts"] == 3
    head = s3.head_object(Bucket="lake", Key=landing_key(DAY))
    assert head["Metadata"]["sha256"] == marker["sha256"]

    prefix = staging_prefix(DAY)
    assert keys(s3, prefix) == [
        prefix + MARKER,
        prefix + "part-00000.txt.gz",
        prefix + "part-00001.txt.gz",
        prefix + "part-00002.txt.gz",
    ]
    first = s3.get_object(Bucket="lake", Key=prefix + "part-00000.txt.gz")["Body"].read()
    lines = gzip.decompress(first).decode("utf-8").splitlines()
    assert len(lines) == 3
    assert "\r" not in lines[0]
    assert "PEÑA" in lines[0]  # read as Latin-1, written as UTF-8


def test_landing_the_same_file_twice_does_nothing(s3, tmp_path):
    make_snapshot(tmp_path, DAY, valid_rows=5)
    first = land(SETTINGS, tmp_path, DAY, s3=s3)
    assert land(SETTINGS, tmp_path, DAY, s3=s3) == first


def test_a_new_file_for_the_same_day_replaces_the_parts(s3, tmp_path):
    make_snapshot(tmp_path, DAY, valid_rows=5)
    land(SETTINGS, tmp_path, DAY, s3=s3, lines_per_part=2)
    make_snapshot(tmp_path, DAY, valid_rows=1)
    marker = land(SETTINGS, tmp_path, DAY, s3=s3, lines_per_part=2)

    assert marker["lines"] == 1 + len(BAD_LINES)
    parts = [key for key in keys(s3, staging_prefix(DAY)) if key.endswith(".txt.gz")]
    assert len(parts) == 2
    stored = s3.get_object(Bucket="lake", Key=staging_prefix(DAY) + MARKER)["Body"].read()
    assert json.loads(stored) == marker


def test_a_changed_header_stops_before_staging(s3, tmp_path):
    path = make_snapshot(tmp_path, DAY, valid_rows=1)
    header = contracts.SOURCE_HEADER.replace("UBIGEO", "UBIGEO_INEI")
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("padron_reducido_ruc.txt", (header + "\r\n").encode("latin-1"))

    with pytest.raises(SchemaDriftError, match="UBIGEO_INEI"):
        land(SETTINGS, tmp_path, DAY, s3=s3)
    assert keys(s3, staging_prefix(DAY)) == []


def test_a_missing_snapshot_is_reported(s3, tmp_path):
    with pytest.raises(FileNotFoundError):
        land(SETTINGS, tmp_path, DAY, s3=s3)
