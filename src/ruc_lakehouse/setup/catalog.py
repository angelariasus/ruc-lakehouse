"""Prepare the local Lakekeeper catalog: bootstrap it and create the warehouse.

Idempotent, so ``docker compose up`` can run it every time:

    python -m ruc_lakehouse.setup.catalog
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Any

from ruc_lakehouse.config import Settings

DEFAULT_PROJECT = "00000000-0000-0000-0000-000000000000"


def management_url(settings: Settings) -> str:
    # CATALOG_URI points at the Iceberg REST root (".../catalog").
    return settings.catalog_uri.rstrip("/").removesuffix("/catalog") + "/management/v1"


def request(method: str, url: str, body: dict[str, Any] | None = None) -> Any:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"{method} {url} -> {error.code}: {detail}") from error
    return json.loads(payload) if payload else None


def warehouse_body(settings: Settings) -> dict[str, Any]:
    """Warehouse on the local S3-compatible store, keys given by the clients.

    Credential vending (STS) and remote signing are off: Spark and Trino bring
    their own keys, as they would bring an IAM role on AWS. Remote signing would
    also be unsafe here, since MinIO-based stores do not check every part of a
    signed request.
    """
    return {
        "warehouse-name": settings.warehouse,
        "project-id": DEFAULT_PROJECT,
        "storage-profile": {
            "type": "s3",
            "bucket": settings.bucket,
            "key-prefix": "warehouse",
            "endpoint": settings.s3_endpoint,
            "region": settings.region,
            "path-style-access": True,
            "flavor": "s3-compat",
            "sts-enabled": False,
            "remote-signing-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": settings.s3_access_key,
            "secret-access-key": settings.s3_secret_key,
        },
    }


def main() -> int:
    settings = Settings.from_env()
    if settings.profile != "local":
        print("Catalog setup only applies to the local profile; nothing to do.")
        return 0

    base = management_url(settings)
    if not request("GET", f"{base}/info").get("bootstrapped"):
        request("POST", f"{base}/bootstrap", {"accept-terms-of-use": True})
        print("Catalog bootstrapped.")

    existing = request("GET", f"{base}/warehouse").get("warehouses", [])
    if any(w.get("name") == settings.warehouse for w in existing):
        print(f"Warehouse {settings.warehouse!r} already exists.")
        return 0

    request("POST", f"{base}/warehouse", warehouse_body(settings))
    print(f"Warehouse {settings.warehouse!r} created on s3://{settings.bucket}/warehouse.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
