from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    temporal_address: str
    temporal_namespace: str
    temporal_task_queue: str
    database_url: str
    discord_webhook_url: str | None
    fred_api_key: str | None
    http_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
            temporal_task_queue=os.getenv("TEMPORAL_TASK_QUEUE", "interest-rates"),
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql+psycopg://finance-tools:finance-tools@postgresql:5432/finance_tools",
            ),
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL"),
            fred_api_key=os.getenv("FRED_API_KEY"),
            http_timeout_seconds=float(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
        )
