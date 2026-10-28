from __future__ import annotations

import os
import re
from dataclasses import dataclass

DEFAULT_JOB_PRIORITIES = {
    "ThetaDataOptionDataWorkflow": 10,
    "ThetaDataSecuritiesWorkflow": 20,
    "ThetaDataOptionBackfillWorkflow": 100,
}
UNKNOWN_JOB_PRIORITY = 1000

_PRIORITY_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")
_REDIS_URL = re.compile(r"^rediss?://[^/\s]+(?:/\d+)?$")


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _integer(name: str, default: int, *, minimum: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer") from error
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}")
    return value


def _redis_url(raw: str) -> str:
    if _REDIS_URL.fullmatch(raw) is None:
        raise RuntimeError("REDIS_URL must start with redis:// or rediss://")
    return raw


def parse_job_priorities(raw: str | None) -> dict[str, int]:
    priorities = dict(DEFAULT_JOB_PRIORITIES)
    if raw is None or not raw.strip():
        return priorities
    for part in raw.split(","):
        piece = part.strip()
        if "=" not in piece:
            raise RuntimeError(
                "THETADATA_JOB_PRIORITIES must be Name=integer pairs"
            )
        name, value = piece.split("=", 1)
        name = name.strip()
        value = value.strip()
        if _PRIORITY_NAME.fullmatch(name) is None:
            raise RuntimeError(
                "THETADATA_JOB_PRIORITIES must be Name=integer pairs"
            )
        try:
            priority = int(value)
        except ValueError as error:
            raise RuntimeError(
                "THETADATA_JOB_PRIORITIES must be Name=integer pairs"
            ) from error
        if priority < 0:
            raise RuntimeError(
                "THETADATA_JOB_PRIORITIES must be Name=integer pairs"
            )
        priorities[name] = priority
    return priorities


@dataclass(frozen=True)
class Settings:
    temporal_address: str
    temporal_namespace: str
    temporal_task_queue: str
    thetadata_api_key: str
    garage_access_key_id: str
    garage_secret_access_key: str
    garage_s3_endpoint: str
    garage_s3_region: str
    quack_uri: str
    quack_token: str
    iceberg_catalog_uri: str
    redis_url: str
    thetadata_max_concurrent: int
    thetadata_requests_per_minute: int
    thetadata_lease_seconds: int
    thetadata_job_priorities: dict[str, int]
    thetadata_option_batch_size: int
    thetadata_option_min_rows: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
            temporal_task_queue=os.getenv(
                "THETADATA_ETL_TEMPORAL_TASK_QUEUE", "thetadata-etl"
            ),
            thetadata_api_key=_required("THETADATA_API_KEY"),
            garage_access_key_id=_required("GARAGE_ACCESS_KEY_ID"),
            garage_secret_access_key=_required("GARAGE_SECRET_ACCESS_KEY"),
            garage_s3_endpoint=os.getenv(
                "GARAGE_S3_ENDPOINT", "http://127.0.0.1:3900"
            ),
            garage_s3_region=os.getenv("GARAGE_S3_REGION", "garage"),
            quack_uri=os.getenv("QUACK_URI", "quack:127.0.0.1:9494"),
            quack_token=_required("QUACK_TOKEN"),
            iceberg_catalog_uri=os.getenv(
                "ICEBERG_CATALOG_URI", "http://192.168.0.20:8181/catalog"
            ),
            redis_url=_redis_url(_required("REDIS_URL")),
            thetadata_max_concurrent=_integer(
                "THETADATA_MAX_CONCURRENT", 2, minimum=1
            ),
            thetadata_requests_per_minute=_integer(
                "THETADATA_REQUESTS_PER_MINUTE", 0, minimum=0
            ),
            thetadata_lease_seconds=_integer(
                "THETADATA_LEASE_SECONDS", 900, minimum=1
            ),
            thetadata_job_priorities=parse_job_priorities(
                os.getenv("THETADATA_JOB_PRIORITIES")
            ),
            thetadata_option_batch_size=_integer(
                "THETADATA_OPTION_BATCH_SIZE", 40, minimum=1
            ),
            thetadata_option_min_rows=_integer(
                "THETADATA_OPTION_MIN_ROWS", 1000, minimum=0
            ),
        )
