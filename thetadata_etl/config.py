from __future__ import annotations

import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


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
        )
