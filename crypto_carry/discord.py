from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

import requests

from crypto_carry.carry import parse_observed_at
from crypto_carry.config import Settings
from crypto_carry.models import CarryRow, CarrySnapshot, SpotPrice

DISCORD_CONTENT_LIMIT = 2000
ASSET_TITLES = (("BTC", "Bitcoin"), ("ETH", "Ether"))


def _format_usd(value: str) -> str:
    amount = Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if amount == amount.to_integral_value():
        return f"${amount:,.0f}"
    return f"${amount:,.2f}"


def _format_percent(value: str) -> str:
    return f"{Decimal(value):.2f}%"


def _format_days(value: str) -> str:
    return f"{Decimal(value):.1f}d"


def _heading(snapshot: CarrySnapshot) -> str:
    observed = parse_observed_at(snapshot.observed_at)
    return f"📈 **Crypto carry** — <t:{int(observed.timestamp())}:F>"


def _spot_line(spot: SpotPrice) -> str:
    return f"• {spot.asset} {_format_usd(spot.price)}"


def _spot_lines(snapshot: CarrySnapshot) -> list[str]:
    order = {asset: index for index, asset in enumerate(("BTC", "ETH"))}
    spots = sorted(
        snapshot.spots, key=lambda spot: order.get(spot.asset, 99)
    )
    return ["", "**Spot (Coinbase)**", *[_spot_line(spot) for spot in spots]]


def _row_line(row: CarryRow) -> str:
    return (
        f"• {row.month_label} ({_format_days(row.days_remaining)})  "
        f"F {_format_usd(row.futures_price)}  "
        f"**{_format_percent(row.apy_percent)}**  {row.direction}"
    )


def _fits(lines: list[str], additions: list[str]) -> bool:
    return len("\n".join([*lines, *additions])) <= DISCORD_CONTENT_LIMIT


def build_digest_messages(
    snapshot: CarrySnapshot, workflow_run_id: str
) -> list[str]:
    heading = _heading(snapshot)
    spots = _spot_lines(snapshot)
    run_line = f"\nRun: `{workflow_run_id}`"
    messages: list[str] = []
    lines = [heading, *spots]
    for asset, title in ASSET_TITLES:
        rows = [row for row in snapshot.rows if row.asset == asset]
        started = False
        for row in rows:
            prefix = [] if started else [f"\n**{title}**"]
            addition = [*prefix, _row_line(row)]
            if _fits(lines, [*addition, run_line]):
                lines.extend(addition)
                started = True
                continue
            messages.append("\n".join([*lines, run_line]))
            lines = [heading, f"\n**{title}**", _row_line(row)]
            started = True
    messages.append("\n".join([*lines, run_line]))
    return messages


def build_failure_message(workflow_run_id: str, error: str) -> str:
    sanitized = " ".join(error.split())[:1000]
    return (
        "⚠️ **Crypto carry failed after retries**\n"
        f"Run: `{workflow_run_id}`\n"
        f"Error: {sanitized}"
    )[:DISCORD_CONTENT_LIMIT]


def send_discord_message(content: str, settings: Settings | None = None) -> None:
    settings = settings or Settings.from_env()
    response = requests.post(
        settings.discord_webhook_url,
        params={"wait": "true"},
        json={"content": content, "allowed_mentions": {"parse": []}},
        headers={"User-Agent": "finance-tools-crypto-carry/0.1"},
        timeout=settings.http_timeout_seconds,
    )
    response.raise_for_status()
