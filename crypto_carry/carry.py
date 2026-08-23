from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from crypto_carry.common import ScrapeError
from crypto_carry.models import CarryRow, CarrySnapshot, FuturesQuote, SpotPrice

MIN_DAYS_TO_EXPIRY = Decimal("7")
ASSETS = ("BTC", "ETH")
SECONDS_PER_DAY = Decimal("86400")
DAYS_PER_YEAR = Decimal("365")


def parse_observed_at(value: str) -> datetime:
    try:
        observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ScrapeError(f"invalid observed_at timestamp: {value!r}") from exc
    if observed.tzinfo is None:
        raise ScrapeError(f"observed_at must be timezone-aware: {value!r}")
    return observed


def _days_remaining(last_trade_at: str, observed_at: datetime) -> Decimal:
    last_trade = datetime.fromisoformat(last_trade_at)
    if last_trade.tzinfo is None:
        last_trade = last_trade.replace(tzinfo=timezone.utc)
    seconds = Decimal(str((last_trade - observed_at).total_seconds()))
    return seconds / SECONDS_PER_DAY


def carry_row(
    quote: FuturesQuote, spot: SpotPrice, observed_at: datetime
) -> CarryRow | None:
    days = _days_remaining(quote.last_trade_at, observed_at)
    if days < MIN_DAYS_TO_EXPIRY:
        return None
    spot_price = Decimal(spot.price)
    futures_price = Decimal(quote.price)
    basis = (futures_price - spot_price) / spot_price
    apy = basis * DAYS_PER_YEAR / days
    if apy >= 0:
        direction = "long/short"
    else:
        direction = "short/long"
        apy = -apy
    return CarryRow(
        asset=quote.asset,
        contract_code=quote.contract_code,
        month_label=quote.month_label,
        expiration=quote.expiration,
        days_remaining=format(days, "f"),
        futures_price=quote.price,
        spot_price=spot.price,
        apy_percent=format(apy * 100, "f"),
        direction=direction,
    )


def build_snapshot(
    observed_at: str,
    spots: list[SpotPrice],
    quotes: list[FuturesQuote],
) -> CarrySnapshot:
    observed = parse_observed_at(observed_at)
    spots_by_asset = {spot.asset: spot for spot in spots}
    for asset in ASSETS:
        if asset not in spots_by_asset:
            raise ScrapeError(f"missing {asset} spot price")

    rows: list[CarryRow] = []
    for quote in quotes:
        if quote.asset not in spots_by_asset:
            raise ScrapeError(f"missing spot price for {quote.asset}")
        row = carry_row(quote, spots_by_asset[quote.asset], observed)
        if row is not None:
            rows.append(row)

    rows.sort(
        key=lambda row: (
            ASSETS.index(row.asset) if row.asset in ASSETS else 99,
            row.expiration,
            row.contract_code,
        )
    )
    for asset in ASSETS:
        if not any(row.asset == asset for row in rows):
            raise ScrapeError(
                f"no {asset} futures with at least 7 days to expiry"
            )
    return CarrySnapshot(observed_at=observed_at, spots=spots, rows=rows)
