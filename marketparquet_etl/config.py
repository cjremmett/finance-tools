from __future__ import annotations

import os
import re
from dataclasses import dataclass

_REDIS_URL = re.compile(r"^rediss?://[^/\s]+(?:/\d+)?$")
_CATALOG_URI = re.compile(r"https?://[A-Za-z0-9._:-]+(?:/[A-Za-z0-9._~/-]*)?")


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _redis_url(raw: str) -> str:
    if _REDIS_URL.fullmatch(raw) is None:
        raise RuntimeError("REDIS_URL must start with redis:// or rediss://")
    return raw


def _requests_per_minute(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(
            "MARKETPARQUET_REQUESTS_PER_MINUTE must be an integer from 1 to 60"
        ) from error
    if value < 1 or value > 60:
        raise RuntimeError(
            "MARKETPARQUET_REQUESTS_PER_MINUTE must be an integer from 1 to 60"
        )
    return value


def require_catalog_uri(catalog_uri: str) -> str:
    if _CATALOG_URI.fullmatch(catalog_uri) is None:
        raise ValueError(f"invalid Iceberg catalog URI: {catalog_uri}")
    return catalog_uri


@dataclass(frozen=True)
class Settings:
    temporal_address: str
    temporal_namespace: str
    temporal_task_queue: str
    marketparquet_api_key: str
    garage_access_key_id: str
    garage_secret_access_key: str
    garage_s3_endpoint: str
    garage_s3_region: str
    quack_uri: str
    quack_token: str
    iceberg_catalog_uri: str
    redis_url: str
    requests_per_minute: int
    discord_webhook_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
            temporal_task_queue=os.getenv(
                "MARKETPARQUET_ETL_TEMPORAL_TASK_QUEUE", "marketparquet-etl"
            ),
            marketparquet_api_key=_required("MARKETPARQUET_API_KEY"),
            garage_access_key_id=_required("GARAGE_ACCESS_KEY_ID"),
            garage_secret_access_key=_required("GARAGE_SECRET_ACCESS_KEY"),
            garage_s3_endpoint=os.getenv(
                "GARAGE_S3_ENDPOINT", "http://127.0.0.1:3900"
            ),
            garage_s3_region=os.getenv("GARAGE_S3_REGION", "garage"),
            quack_uri=os.getenv("QUACK_URI", "quack:127.0.0.1:9494"),
            quack_token=_required("QUACK_TOKEN"),
            iceberg_catalog_uri=require_catalog_uri(
                os.getenv(
                    "ICEBERG_CATALOG_URI", "http://192.168.0.20:8181/catalog"
                )
            ),
            redis_url=_redis_url(_required("REDIS_URL")),
            requests_per_minute=_requests_per_minute(
                os.getenv("MARKETPARQUET_REQUESTS_PER_MINUTE", "50").strip()
            ),
            discord_webhook_url=_required("ETL_PIPELINE_DISCORD_WEBHOOK"),
        )
