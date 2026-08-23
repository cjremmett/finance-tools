from __future__ import annotations

from temporalio import activity

from crypto_carry.config import Settings
from crypto_carry.discord import send_discord_message
from crypto_carry.models import CarrySnapshot, FetchRequest
from crypto_carry.snapshot import fetch_carry_snapshot


@activity.defn(name="fetch_crypto_carry")
def fetch_crypto_carry_activity(request: FetchRequest) -> CarrySnapshot:
    settings = Settings.from_env()
    return fetch_carry_snapshot(request.observed_at, settings.http_timeout_seconds)


@activity.defn(name="send_crypto_carry_discord_message")
def send_crypto_carry_discord_message_activity(content: str) -> None:
    send_discord_message(content)
