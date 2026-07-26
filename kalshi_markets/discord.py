from __future__ import annotations

import time

import requests

from kalshi_markets.config import Settings
from kalshi_markets.models import NewMarket

DISCORD_CONTENT_LIMIT = 2000
MESSAGE_HEADING = "🆕 **New Kalshi markets**"


def _market_line(market: NewMarket) -> str:
    suffix = ""
    if market.subtitle and market.subtitle.casefold() not in market.title.casefold():
        suffix = f" — {market.subtitle}"
    return f"• **{market.title}**{suffix}"


def _fit_line(line: str, available: int) -> str:
    if len(line) <= available:
        return line
    if available <= 1:
        return "…"[:available]
    return f"{line[: available - 1]}…"


def build_market_messages(markets: list[NewMarket]) -> list[str]:
    if not markets:
        return [
            "✅ **Kalshi market monitor completed**\n"
            "No new markets were detected in the configured categories."
        ]

    grouped: dict[str, list[NewMarket]] = {}
    for market in markets:
        grouped.setdefault(market.category, []).append(market)

    messages: list[str] = []
    lines = [MESSAGE_HEADING]
    has_market_line = False
    for category in sorted(grouped):
        category_header = f"\n**{category}**"
        category_started = False
        for market in sorted(
            grouped[category], key=lambda item: (item.title, item.ticker)
        ):
            raw_line = _market_line(market)
            candidate = lines + (
                [] if category_started else [category_header]
            ) + [raw_line]
            if (
                len("\n".join(candidate)) > DISCORD_CONTENT_LIMIT
                and has_market_line
            ):
                messages.append("\n".join(lines))
                lines = [MESSAGE_HEADING, category_header]
                category_started = True
                has_market_line = False
            elif not category_started:
                lines.append(category_header)
                category_started = True
            available = DISCORD_CONTENT_LIMIT - len("\n".join(lines)) - 1
            lines.append(_fit_line(raw_line, available))
            has_market_line = True
    messages.append("\n".join(lines))
    return messages


def build_initialization_message() -> str:
    return (
        "✅ **Kalshi market monitor initialized**\n"
        "No existing markets were backfilled. Future runs will report newly "
        "created markets."
    )


def build_new_category_messages(categories: list[str]) -> list[str]:
    if not categories:
        return []
    heading = "🆕 **New Kalshi categories released**"
    messages: list[str] = []
    lines = [heading]
    for category in sorted(categories):
        line = f"• **{category}**"
        if len("\n".join(lines + [line])) > DISCORD_CONTENT_LIMIT:
            messages.append("\n".join(lines))
            lines = [heading]
        available = DISCORD_CONTENT_LIMIT - len("\n".join(lines)) - 1
        lines.append(_fit_line(line, available))
    messages.append("\n".join(lines))
    return messages


def build_failure_message(
    workflow_run_id: str, stage: str, error: str
) -> str:
    sanitized = " ".join(error.split())[:1000]
    return (
        "⚠️ **Kalshi market monitor failed after retries**\n"
        f"Stage: `{stage}`\n"
        f"Run: `{workflow_run_id}`\n"
        f"Error: {sanitized}"
    )[:DISCORD_CONTENT_LIMIT]


def send_discord_message(content: str, settings: Settings | None = None) -> None:
    settings = settings or Settings.from_env()
    for attempt in range(6):
        response = requests.post(
            settings.discord_webhook_url,
            params={"wait": "true"},
            json={"content": content, "allowed_mentions": {"parse": []}},
            headers={"User-Agent": "finance-tools-kalshi-markets/0.1"},
            timeout=settings.http_timeout_seconds,
        )
        if response.status_code != 429:
            response.raise_for_status()
            return
        if attempt == 5:
            response.raise_for_status()
        retry_after = response.headers.get("Retry-After")
        if retry_after is None:
            try:
                retry_after = str(response.json().get("retry_after", 1))
            except requests.JSONDecodeError:
                retry_after = "1"
        time.sleep(max(float(retry_after), 0))
