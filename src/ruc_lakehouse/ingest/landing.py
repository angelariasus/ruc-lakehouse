"""Land one downloaded snapshot in object storage.

    python -m ruc_lakehouse.ingest.landing 2026-10-25

Reads ``<SNAPSHOTS_DIR>/raw/snapshot_date=<date>/padron_reducido_ruc.zip``
(the layout of the daily collector) and:

1. uploads the zip unchanged to ``landing/``, with its SHA-256 as metadata;
2. checks the header against the contract, so a schema change stops here;
3. re-chunks the lines into UTF-8 gzip parts under ``staging/``, because Spark
   cannot split a zip file and would read 1.6 GB in a single task;
4. writes ``_SUCCESS.json`` with the line count, which bronze reconciles.

Running it again for a snapshot already landed with the same SHA-256 does
nothing.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import sys
import tempfile
import zipfile
from collections.abc import Iterator
from datetime import UTC, date, datetime
from itertools import islice
from pathlib import Path
from typing import Any

from ruc_lakehouse import contracts
from ruc_lakehouse.config import Settings
from ruc_lakehouse.storage import MARKER, SOURCE_FILE, landing_key, s3_client, staging_prefix

LINES_PER_PART = 1_000_000


class SchemaDriftError(RuntimeError):
    """The source header no longer matches the contract."""


def snapshot_path(snapshots_dir: Path, snapshot_date: date) -> Path:
    return snapshots_dir / "raw" / f"snapshot_date={snapshot_date.isoformat()}" / SOURCE_FILE


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def read_marker(s3: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(body)


def source_lines(zip_path: Path) -> Iterator[str]:
    """Data lines of the single text file in the zip, header checked and dropped."""
    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        if len(names) != 1:
            raise SchemaDriftError(f"expected one file in the zip, found {names}")
        with archive.open(names[0]) as raw:
            text = io.TextIOWrapper(raw, encoding=contracts.SOURCE_ENCODING, newline=None)
            header = text.readline().rstrip("\n")
            if header != contracts.SOURCE_HEADER:
                raise SchemaDriftError(f"unexpected header: {header!r}")
            for line in text:
                yield line.rstrip("\n")


def delete_prefix(s3: Any, bucket: str, prefix: str) -> None:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def land(
    settings: Settings,
    snapshots_dir: Path,
    snapshot_date: date,
    s3: Any = None,
    lines_per_part: int = LINES_PER_PART,
) -> dict[str, Any]:
    s3 = s3 or s3_client(settings)
    zip_path = snapshot_path(snapshots_dir, snapshot_date)
    if not zip_path.is_file():
        raise FileNotFoundError(zip_path)

    sha256 = sha256_of(zip_path)
    prefix = staging_prefix(snapshot_date)
    marker = read_marker(s3, settings.bucket, prefix + MARKER)
    if marker and marker["sha256"] == sha256:
        print(f"{snapshot_date}: already landed ({marker['lines']:,} lines), nothing to do")
        return marker

    s3.upload_file(
        str(zip_path),
        settings.bucket,
        landing_key(snapshot_date),
        ExtraArgs={"Metadata": {"sha256": sha256}},
    )

    # A previous run may have left parts from a different file or chunking.
    delete_prefix(s3, settings.bucket, prefix)
    lines = source_lines(zip_path)
    total = parts = 0
    with tempfile.TemporaryDirectory() as tmp:
        while chunk := list(islice(lines, lines_per_part)):
            part = Path(tmp) / f"part-{parts:05d}.txt.gz"
            with gzip.open(part, "wt", encoding="utf-8", compresslevel=1) as out:
                out.write("\n".join(chunk) + "\n")
            s3.upload_file(str(part), settings.bucket, prefix + part.name)
            part.unlink()
            total += len(chunk)
            parts += 1

    marker = {
        "snapshot_date": snapshot_date.isoformat(),
        "sha256": sha256,
        "source_bytes": zip_path.stat().st_size,
        "lines": total,
        "parts": parts,
        "landed_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    s3.put_object(Bucket=settings.bucket, Key=prefix + MARKER, Body=json.dumps(marker).encode())
    print(f"{snapshot_date}: landed {total:,} lines in {parts} part(s)")
    return marker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("snapshot_date", type=date.fromisoformat)
    parser.add_argument(
        "--snapshots-dir",
        type=Path,
        default=Path(os.environ.get("SNAPSHOTS_DIR", "/snapshots")),
    )
    args = parser.parse_args(argv)
    land(Settings.from_env(), args.snapshots_dir, args.snapshot_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
