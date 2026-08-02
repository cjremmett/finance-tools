from __future__ import annotations

import time

import requests

from kalshi_markets.config import Settings
from kalshi_markets.models import NewMarket

DISCORD_CONTENT_LIMIT = 2000
MESSAGE_HEADING = "🆕 **New Kalshi markets**"


def _event_title(markets: list[NewMarket], *, continued: bool = False) -> str:
    market = min(markets, key=lambda item: (item.title.casefold(), item.ticker))
    suffix = " *(continued)*" if continued else ""
    return f"• **{market.title}**{suffix}"


def _variant_line(market: NewMarket) -> str:
    return f"-# • {market.subtitle or market.ticker}"


def _fit_line(line: str, available: int) -> str:
    if len(line) <= available:
        return line
    if available <= 1:
        return "…"[:available]
    return f"{line[: available - 1]}…"


def _fits(lines: list[str], additions: list[str]) -> bool:
    return len("\n".join([*lines, *additions])) <= DISCORD_CONTENT_LIMIT


def _start_event(
    lines: list[str],
    event_markets: list[NewMarket],
    first_variant: str,
    *,
    continued: bool,
) -> None:
    raw_title = _event_title(event_markets, continued=continued)
    reserved_for_variant = min(len(first_variant), 1)
    available = (
        DISCORD_CONTENT_LIMIT
        - len("\n".join(lines))
        - 2
        - reserved_for_variant
    )
    lines.append(_fit_line(raw_title, available))


def build_market_messages(markets: list[NewMarket]) -> list[str]:
    if not markets:
        return [
            "✅ **Kalshi market monitor completed**\n"
            "No new markets were detected in the configured categories."
        ]

    grouped: dict[str, dict[str, list[NewMarket]]] = {}
    for market in markets:
        grouped.setdefault(market.category, {}).setdefault(
            market.event_ticker, []
        ).append(market)

    messages: list[str] = []
    lines = [MESSAGE_HEADING]
    has_event = False
    for category in sorted(grouped):
        category_header = f"\n**{category}**"
        category_started = False
        events = sorted(
            grouped[category].values(),
            key=lambda items: (
                min(items, key=lambda item: (item.title.casefold(), item.ticker))
                .title.casefold(),
                items[0].event_ticker,
            ),
        )
        for event_markets in events:
            event_markets = sorted(
                event_markets,
                key=lambda item: (
                    (item.subtitle or item.ticker).casefold(),
                    item.ticker,
                ),
            )
            event_lines = [
                _event_title(event_markets),
                *[_variant_line(market) for market in event_markets],
            ]
            prefix = [] if category_started else [category_header]
            if _fits(lines, [*prefix, *event_lines]):
                lines.extend([*prefix, *event_lines])
                category_started = True
                has_event = True
                continue

            if has_event:
                messages.append("\n".join(lines))
                lines = [MESSAGE_HEADING, category_header]
                category_started = True
                has_event = False
            elif not category_started:
                lines.append(category_header)
                category_started = True

            if _fits(lines, event_lines):
                lines.extend(event_lines)
                has_event = True
                continue

            variant_lines = event_lines[1:]
            _start_event(
                lines,
                event_markets,
                variant_lines[0],
                continued=False,
            )
            variant_started = False
            for variant_line in variant_lines:
                if not _fits(lines, [variant_line]) and variant_started:
                    messages.append("\n".join(lines))
                    lines = [MESSAGE_HEADING, category_header]
                    _start_event(
                        lines,
                        event_markets,
                        variant_line,
                        continued=True,
                    )
                available = DISCORD_CONTENT_LIMIT - len("\n".join(lines)) - 1
                lines.append(_fit_line(variant_line, available))
                variant_started = True
            has_event = True
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
