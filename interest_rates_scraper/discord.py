from __future__ import annotations

import re
from decimal import Decimal

import requests

from interest_rates_scraper.config import Settings
from interest_rates_scraper.models import DiscordNotification, RateChange

SOURCE_ORDER = {
    "federal_reserve": 0,
    "marcus_savings": 1,
    "marcus_cds": 2,
    "kalshi": 3,
}

SOURCE_LABELS = {
    "federal_reserve": "Federal Reserve",
    "marcus_savings": "Marcus Savings",
    "marcus_cds": "Marcus CDs",
    "kalshi": "Kalshi",
}


def _format_percent(value: str | None) -> str:
    if value is None:
        raise ValueError("a percentage value is required")
    return f"{Decimal(value):.2f}%"


def _change_sort_key(change: RateChange) -> tuple[int, str]:
    if change.source != "marcus_cds":
        return (0, change.product_name)
    term_match = re.search(r"Marcus\s+(\d+)-Month\b", change.product_name)
    term_months = int(term_match.group(1)) if term_match else 10**9
    return (term_months, change.product_name)


def _change_line(change: RateChange) -> str:
    if change.change_type == "changed":
        return (
            f"• **{change.product_name}**: "
            f"{_format_percent(change.old_rate_percent)} → "
            f"{_format_percent(change.new_rate_percent)}"
        )
    if change.change_type == "removed":
        return (
            f"• **{change.product_name}** removed "
            f"(was {_format_percent(change.old_rate_percent)})"
        )
    return (
        f"• **{change.product_name}** added at "
        f"{_format_percent(change.new_rate_percent)}"
    )


def build_discord_message(notification: DiscordNotification) -> str:
    heading = (
        "📊 **Interest-rate baseline**"
        if notification.baseline
        else "📈 **Interest-rate update**"
    )
    sections = [heading]
    if notification.changes:
        grouped: dict[str, list[RateChange]] = {}
        for change in notification.changes:
            grouped.setdefault(change.source, []).append(change)
        ordered_sources = sorted(
            grouped, key=lambda source: (SOURCE_ORDER.get(source, 10**9), source)
        )
        for source in ordered_sources:
            sections.append(
                f"\n**{SOURCE_LABELS.get(source, source.replace('_', ' ').title())}**"
            )
            sections.extend(
                _change_line(change)
                for change in sorted(grouped[source], key=_change_sort_key)
            )
    if notification.failures:
        sections.append("\n⚠️ **Scrape failures after retries**")
        sections.extend(
            f"• **{failure.source}**: {failure.error[:300]}"
            for failure in notification.failures
        )
    sections.append(f"\nRun: `{notification.workflow_run_id}`")
    return "\n".join(sections)[:2000]


def send_discord(notification: DiscordNotification) -> None:
    settings = Settings.from_env()
    if not settings.discord_webhook_url:
        raise RuntimeError("DISCORD_WEBHOOK_URL is required to send notifications")
    response = requests.post(
        settings.discord_webhook_url,
        params={"wait": "true"},
        json={
            "content": build_discord_message(notification),
            "allowed_mentions": {"parse": []},
        },
        timeout=settings.http_timeout_seconds,
    )
    response.raise_for_status()
