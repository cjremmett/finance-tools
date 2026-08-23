from __future__ import annotations

import re
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from crypto_carry.common import ScrapeError, fetch_json
from crypto_carry.models import FuturesQuote

TRADINGVIEW_SCAN_URL = "https://scanner.tradingview.com/futures/scan"
SCAN_COLUMNS = ("name", "close", "description", "expiration", "exchange")
SYMBOL_RE = re.compile(r"^CME:(BTC|ETH)([FGHJKMNQUVXZ])(\d{4})$")
MONTH_CODES = {
    "F": "JAN",
    "G": "FEB",
    "H": "MAR",
    "J": "APR",
    "K": "MAY",
    "M": "JUN",
    "N": "JUL",
    "Q": "AUG",
    "U": "SEP",
    "V": "OCT",
    "X": "NOV",
    "Z": "DEC",
}
LONDON = ZoneInfo("Europe/London")
LAST_TRADE_TIME = time(16, 0)
ASSET_SCANS = (
    ("BTC", "Bitcoin Futures"),
    ("ETH", "Ether Futures"),
)


def _scan_payload(description_match: str) -> dict[str, Any]:
    return {
        "columns": list(SCAN_COLUMNS),
        "range": [0, 200],
        "filter": [
            {"left": "exchange", "operation": "equal", "right": "CME"},
            {
                "left": "description",
                "operation": "match",
                "right": description_match,
            },
        ],
    }


def _parse_expiration(value: Any, symbol: str) -> date:
    if isinstance(value, int) and value > 0:
        text = f"{value:08d}"
    elif isinstance(value, str) and value.isdigit() and len(value) == 8:
        text = value
    else:
        raise ScrapeError(
            f"missing or invalid expiration for {symbol}: {value!r}"
        )
    try:
        return date.fromisoformat(f"{text[0:4]}-{text[4:6]}-{text[6:8]}")
    except ValueError as exc:
        raise ScrapeError(
            f"unparseable expiration for {symbol}: {value!r}"
        ) from exc


def _parse_close(value: Any, symbol: str) -> Decimal:
    if value is None or value == "":
        raise ScrapeError(f"missing close for {symbol}")
    try:
        price = Decimal(str(value))
    except InvalidOperation as exc:
        raise ScrapeError(f"invalid close for {symbol}: {value!r}") from exc
    if price <= 0:
        raise ScrapeError(f"non-positive close for {symbol}: {value!r}")
    return price


def _quote_from_row(
    symbol: str, match: re.Match[str], fields: dict[str, Any]
) -> FuturesQuote:
    asset = match.group(1)
    month_code = match.group(2)
    year = match.group(3)
    expiration = _parse_expiration(fields.get("expiration"), symbol)
    last_trade = datetime.combine(expiration, LAST_TRADE_TIME, tzinfo=LONDON)
    return FuturesQuote(
        asset=asset,
        symbol=symbol,
        contract_code=f"{asset}{month_code}{year}",
        month_label=f"{MONTH_CODES[month_code]} {year}",
        expiration=expiration.isoformat(),
        last_trade_at=last_trade.isoformat(),
        price=format(_parse_close(fields.get("close"), symbol), "f"),
    )


def parse_futures_scan(payload: Any) -> list[FuturesQuote]:
    if not isinstance(payload, dict):
        raise ScrapeError("invalid TradingView scanner response")
    error = payload.get("errmsg") or payload.get("error")
    if error:
        raise ScrapeError(f"TradingView scanner error: {error}")
    data = payload.get("data")
    if not isinstance(data, list):
        raise ScrapeError("scanner response missing data list")

    quotes: list[FuturesQuote] = []
    for row in data:
        if not isinstance(row, dict):
            raise ScrapeError("invalid scanner row")
        symbol = row.get("s")
        values = row.get("d")
        if not isinstance(symbol, str):
            raise ScrapeError("scanner row missing symbol")
        match = SYMBOL_RE.fullmatch(symbol)
        if match is None:
            continue
        if not isinstance(values, list):
            raise ScrapeError(f"invalid scanner fields for {symbol}")
        fields = {
            name: values[index] if index < len(values) else None
            for index, name in enumerate(SCAN_COLUMNS)
        }
        quotes.append(_quote_from_row(symbol, match, fields))
    return quotes


def fetch_cme_futures(timeout_seconds: float) -> list[FuturesQuote]:
    quotes_by_symbol: dict[str, FuturesQuote] = {}
    for asset, description in ASSET_SCANS:
        payload = fetch_json(
            "POST",
            TRADINGVIEW_SCAN_URL,
            timeout_seconds,
            payload=_scan_payload(description),
        )
        parsed = parse_futures_scan(payload)
        asset_quotes = [quote for quote in parsed if quote.asset == asset]
        if not asset_quotes:
            raise ScrapeError(
                f"no standard CME {asset} futures in scanner response"
            )
        for quote in asset_quotes:
            quotes_by_symbol[quote.symbol] = quote

    quotes = list(quotes_by_symbol.values())
    if not any(quote.asset == "BTC" for quote in quotes):
        raise ScrapeError("no standard CME Bitcoin futures in scanner response")
    if not any(quote.asset == "ETH" for quote in quotes):
        raise ScrapeError("no standard CME Ether futures in scanner response")
    return quotes
