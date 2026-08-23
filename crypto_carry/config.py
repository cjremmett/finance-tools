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
    discord_webhook_url: str
    http_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
            temporal_task_queue=os.getenv(
                "CRYPTO_CARRY_TEMPORAL_TASK_QUEUE", "crypto-carry"
            ),
            discord_webhook_url=_required("CRYPTO_CARRY_DISCORD_WEBHOOK_URL"),
            http_timeout_seconds=float(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
        )
