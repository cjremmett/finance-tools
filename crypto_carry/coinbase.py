from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from crypto_carry.common import ScrapeError, fetch_json
from crypto_carry.models import SpotPrice

COINBASE_SPOT_URL = "https://api.coinbase.com/v2/prices/{product}/spot"
SPOT_ASSETS = ("BTC", "ETH")


def parse_coinbase_spot(
    payload: Any, asset: str, source_url: str
) -> SpotPrice:
    if not isinstance(payload, dict):
        raise ScrapeError(f"invalid Coinbase spot response for {asset}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ScrapeError(f"invalid Coinbase spot payload for {asset}")
    amount = data.get("amount")
    if not isinstance(amount, str) or not amount.strip():
        raise ScrapeError(f"missing Coinbase spot amount for {asset}")
    try:
        price = Decimal(amount.replace(",", "").strip())
    except InvalidOperation as exc:
        raise ScrapeError(
            f"invalid Coinbase spot amount for {asset}: {amount!r}"
        ) from exc
    if price <= 0:
        raise ScrapeError(
            f"non-positive Coinbase spot for {asset}: {amount!r}"
        )
    return SpotPrice(
        asset=asset,
        price=format(price, "f"),
        source="coinbase",
        source_url=source_url,
    )


def fetch_coinbase_spot(asset: str, timeout_seconds: float) -> SpotPrice:
    if asset not in SPOT_ASSETS:
        raise ScrapeError(f"unsupported spot asset: {asset}")
    url = COINBASE_SPOT_URL.format(product=f"{asset}-USD")
    payload = fetch_json("GET", url, timeout_seconds)
    return parse_coinbase_spot(payload, asset, url)


def fetch_coinbase_spots(timeout_seconds: float) -> list[SpotPrice]:
    return [
        fetch_coinbase_spot(asset, timeout_seconds) for asset in SPOT_ASSETS
    ]
