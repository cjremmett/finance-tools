from __future__ import annotations

import requests

from interest_rates_scraper.config import Settings
from interest_rates_scraper.models import DiscordNotification, RateChange


def _change_line(change: RateChange) -> str:
    if change.change_type == "changed":
        return (
            f"• **{change.product_name}**: "
            f"{change.old_rate_percent}% → {change.new_rate_percent}%"
        )
    if change.change_type == "removed":
        return f"• **{change.product_name}** removed (was {change.old_rate_percent}%)"
    return f"• **{change.product_name}** added at {change.new_rate_percent}%"


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
        for source, changes in sorted(grouped.items()):
            sections.append(f"\n**{source.replace('_', ' ').title()}**")
            sections.extend(_change_line(change) for change in changes)
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
