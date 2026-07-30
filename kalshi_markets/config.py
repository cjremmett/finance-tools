from __future__ import annotations

import math
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
    database_url: str
    discord_webhook_url: str
    kalshi_api_key_id: str
    kalshi_private_key_base64: str
    category_whitelist: tuple[str, ...]
    http_timeout_seconds: float
    discord_batch_delay_seconds: float
    kalshi_requests_per_second: float
    kalshi_event_resolution_timeout_seconds: float
    minimum_market_duration_days: float

    @classmethod
    def from_env(cls) -> "Settings":
        categories = tuple(
            dict.fromkeys(
                item.strip()
                for item in _required("KALSHI_CATEGORY_WHITELIST").split(",")
                if item.strip()
            )
        )
        if not categories:
            raise RuntimeError("KALSHI_CATEGORY_WHITELIST must not be empty")
        batch_delay = float(os.getenv("KALSHI_DISCORD_BATCH_DELAY_SECONDS", "1"))
        requests_per_second = float(os.getenv("KALSHI_REQUESTS_PER_SECOND", "10"))
        event_resolution_timeout = float(
            os.getenv("KALSHI_EVENT_RESOLUTION_TIMEOUT_SECONDS", "60")
        )
        minimum_duration_raw = os.getenv(
            "KALSHI_MIN_MARKET_DURATION_DAYS", "0"
        )
        try:
            minimum_duration_days = float(minimum_duration_raw)
        except ValueError as error:
            raise RuntimeError(
                "KALSHI_MIN_MARKET_DURATION_DAYS must be a non-negative number"
            ) from error
        if batch_delay < 0:
            raise RuntimeError(
                "KALSHI_DISCORD_BATCH_DELAY_SECONDS must be non-negative"
            )
        if requests_per_second <= 0:
            raise RuntimeError("KALSHI_REQUESTS_PER_SECOND must be positive")
        if event_resolution_timeout <= 0:
            raise RuntimeError(
                "KALSHI_EVENT_RESOLUTION_TIMEOUT_SECONDS must be positive"
            )
        if not math.isfinite(minimum_duration_days) or minimum_duration_days < 0:
            raise RuntimeError(
                "KALSHI_MIN_MARKET_DURATION_DAYS must be a non-negative number"
            )
        return cls(
            temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
            temporal_task_queue=os.getenv(
                "KALSHI_TEMPORAL_TASK_QUEUE", "kalshi-markets"
            ),
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql+psycopg://finance-tools:finance-tools@postgresql:5432/finance_tools",
            ),
            discord_webhook_url=_required("KALSHI_DISCORD_WEBHOOK_URL"),
            kalshi_api_key_id=_required("KALSHI_API_KEY_ID"),
            kalshi_private_key_base64=_required("KALSHI_PRIVATE_KEY_BASE64"),
            category_whitelist=categories,
            http_timeout_seconds=float(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
            discord_batch_delay_seconds=batch_delay,
            kalshi_requests_per_second=requests_per_second,
            kalshi_event_resolution_timeout_seconds=event_resolution_timeout,
            minimum_market_duration_days=minimum_duration_days,
        )
