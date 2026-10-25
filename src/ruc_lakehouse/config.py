"""Runtime settings, read from environment variables.

One code base, two profiles:

- ``local``: S3-compatible storage and an Iceberg REST catalog (Lakekeeper) in
  Docker, reached with static keys.
- ``aws``: Amazon S3 and the AWS Glue Iceberg REST endpoint, with credentials
  from the default AWS chain (IAM role, profile or environment).

Nothing else in the package needs to know which one is active.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

Profile = Literal["local", "aws"]
PROFILES: tuple[Profile, ...] = ("local", "aws")


class SettingsError(ValueError):
    """Raised when the environment does not describe a usable profile."""


@dataclass(frozen=True)
class Settings:
    profile: Profile
    bucket: str
    region: str
    catalog_uri: str
    warehouse: str
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    @property
    def lake_root(self) -> str:
        return f"s3://{self.bucket}"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env

        def get(name: str) -> str | None:
            return env.get(name, "").strip() or None

        def require(name: str) -> str:
            value = get(name)
            if value is None:
                raise SettingsError(f"{name} is required for the {profile!r} profile")
            return value

        profile = get("LAKE_PROFILE") or "local"
        if profile not in PROFILES:
            raise SettingsError(f"LAKE_PROFILE must be one of {PROFILES}, got {profile!r}")

        bucket = require("LAKE_BUCKET")
        region = get("AWS_REGION") or "us-east-1"

        if profile == "local":
            return cls(
                profile="local",
                bucket=bucket,
                region=region,
                catalog_uri=require("CATALOG_URI"),
                warehouse=require("CATALOG_WAREHOUSE"),
                s3_endpoint=require("S3_ENDPOINT"),
                s3_access_key=require("S3_ACCESS_KEY"),
                s3_secret_key=require("S3_SECRET_KEY"),
            )

        # Glue exposes the Iceberg REST API; the warehouse is the AWS account id.
        return cls(
            profile="aws",
            bucket=bucket,
            region=region,
            catalog_uri=get("CATALOG_URI") or f"https://glue.{region}.amazonaws.com/iceberg",
            warehouse=require("CATALOG_WAREHOUSE"),
        )
