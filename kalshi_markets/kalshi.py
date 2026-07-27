from __future__ import annotations

import base64
import logging
import math
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from kalshi_markets.config import Settings
from kalshi_markets.models import MarketFetchResult, NewMarket

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
EVENT_RETRY_INITIAL_DELAY_SECONDS = 5.0
EVENT_RETRY_MAX_DELAY_SECONDS = 60.0
SECONDS_PER_DAY = 24 * 60 * 60
LOGGER = logging.getLogger(__name__)


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _meets_minimum_duration(
    row: dict[str, Any], minimum_duration_days: float
) -> bool:
    if minimum_duration_days <= 0:
        return True

    ticker = str(row.get("ticker") or "<unknown>")
    open_time = row.get("open_time")
    close_time = row.get("close_time")
    if not open_time or not close_time:
        LOGGER.warning(
            "Keeping Kalshi market %s because open_time or close_time is missing",
            ticker,
        )
        return True

    try:
        opens_at = _parse_datetime(str(open_time))
        closes_at = _parse_datetime(str(close_time))
    except (TypeError, ValueError):
        LOGGER.warning(
            "Keeping Kalshi market %s because its duration timestamps are invalid",
            ticker,
        )
        return True

    duration_seconds = (closes_at - opens_at).total_seconds()
    if duration_seconds < 0:
        LOGGER.warning(
            "Keeping Kalshi market %s because close_time precedes open_time",
            ticker,
        )
        return True
    return duration_seconds >= minimum_duration_days * SECONDS_PER_DAY


class KalshiClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None):
        self.settings = settings
        self.session = session or requests.Session()
        key_bytes = base64.b64decode(settings.kalshi_private_key_base64)
        private_key = serialization.load_pem_private_key(key_bytes, password=None)
        if not isinstance(private_key, rsa.RSAPrivateKey):
            raise RuntimeError("KALSHI_PRIVATE_KEY_BASE64 must contain an RSA key")
        self.private_key = private_key
        self._last_request_at = 0.0

    def _headers(self, method: str, url: str) -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        path = urlparse(url).path
        message = f"{timestamp}{method.upper()}{path}".encode()
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.settings.kalshi_api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
            "User-Agent": "finance-tools-kalshi-markets/0.1",
        }

    def _get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        url = f"{BASE_URL}{path}"
        for attempt in range(6):
            minimum_interval = 1 / self.settings.kalshi_requests_per_second
            remaining = minimum_interval - (time.monotonic() - self._last_request_at)
            if remaining > 0:
                time.sleep(remaining)
            response = self.session.get(
                url,
                params=params,
                headers=self._headers("GET", url),
                timeout=self.settings.http_timeout_seconds,
            )
            self._last_request_at = time.monotonic()
            if response.status_code != 429:
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError(f"unexpected Kalshi response for {path}")
                return payload
            if attempt == 5:
                response.raise_for_status()
            time.sleep(min(0.5 * (2**attempt), 10))
        raise RuntimeError("unreachable Kalshi retry state")

    def _paginate(
        self,
        path: str,
        item_key: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor
            payload = self._get(path, page_params)
            page_items = payload.get(item_key)
            if not isinstance(page_items, list):
                raise RuntimeError(f"Kalshi response omitted {item_key}")
            items.extend(page_items)
            cursor = payload.get("cursor") or None
            if not cursor:
                return items

    def fetch_categories(self) -> list[str]:
        taxonomy = self._get("/search/tags_by_categories").get(
            "tags_by_categories"
        )
        if not isinstance(taxonomy, dict):
            raise RuntimeError("Kalshi category taxonomy is unavailable")
        categories = sorted(str(category) for category in taxonomy)
        unknown = set(self.settings.category_whitelist) - set(categories)
        if unknown:
            raise RuntimeError(
                "unknown Kalshi categories: " + ", ".join(sorted(unknown))
            )
        return categories

    def _fetch_events(
        self, event_tickers: list[str]
    ) -> dict[str, dict[str, Any]]:
        events: dict[str, dict[str, Any]] = {}
        deadline = (
            time.monotonic()
            + self.settings.kalshi_event_resolution_timeout_seconds
        )
        delay = EVENT_RETRY_INITIAL_DELAY_SECONDS
        last_error: Exception | None = None

        while True:
            unresolved = sorted(set(event_tickers) - set(events))
            try:
                for offset in range(0, len(unresolved), 50):
                    chunk = unresolved[offset : offset + 50]
                    rows = self._paginate(
                        "/events",
                        "events",
                        {"limit": 200, "tickers": ",".join(chunk)},
                    )
                    for row in rows:
                        if row.get("event_ticker"):
                            events[str(row["event_ticker"])] = row
                last_error = None
            except (requests.RequestException, RuntimeError) as error:
                last_error = error

            unresolved = sorted(set(event_tickers) - set(events))
            if not unresolved:
                return events

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                detail = (
                    f"; last endpoint error: {last_error}"
                    if last_error is not None
                    else ""
                )
                raise RuntimeError(
                    "Kalshi events could not be resolved after "
                    f"{self.settings.kalshi_event_resolution_timeout_seconds:g} "
                    "seconds: "
                    + ", ".join(unresolved[:10])
                    + detail
                ) from last_error

            time.sleep(min(delay, remaining))
            delay = min(delay * 2, EVENT_RETRY_MAX_DELAY_SECONDS)

    def fetch_new_markets(
        self, window_start: datetime, window_end: datetime
    ) -> MarketFetchResult:
        lower_bound = math.floor(window_start.timestamp()) - 1
        upper_bound = math.ceil(window_end.timestamp()) + 1
        market_rows = self._paginate(
            "/markets",
            "markets",
            {
                "limit": 1000,
                "min_created_ts": lower_bound,
                "max_created_ts": upper_bound,
                "mve_filter": "exclude",
            },
        )
        market_rows = [
            row
            for row in market_rows
            if row.get("created_time")
            and window_start
            < _parse_datetime(str(row["created_time"]))
            <= window_end
        ]

        categories = self.fetch_categories()
        market_rows = [
            row
            for row in market_rows
            if _meets_minimum_duration(
                row, self.settings.minimum_market_duration_days
            )
        ]
        if not market_rows:
            return MarketFetchResult(markets=[], categories=categories)

        event_tickers = sorted(
            {str(row["event_ticker"]) for row in market_rows if row.get("event_ticker")}
        )
        events = self._fetch_events(event_tickers)

        series_payload = self._get("/series")
        series_rows = series_payload.get("series")
        if not isinstance(series_rows, list):
            raise RuntimeError("Kalshi series catalog is unavailable")
        series_categories = {
            str(row["ticker"]): str(row["category"])
            for row in series_rows
            if row.get("ticker") and row.get("category")
        }

        results: list[NewMarket] = []
        for row in market_rows:
            event_ticker = str(row["event_ticker"])
            series_ticker = events[event_ticker].get("series_ticker")
            if not series_ticker or series_ticker not in series_categories:
                raise RuntimeError(
                    f"Kalshi series could not be resolved for {event_ticker}"
                )
            category = series_categories[str(series_ticker)]
            if category not in self.settings.category_whitelist:
                continue
            results.append(
                NewMarket(
                    ticker=str(row["ticker"]),
                    event_ticker=event_ticker,
                    series_ticker=str(series_ticker),
                    title=str(row.get("title") or row["ticker"]),
                    subtitle=(
                        str(row.get("yes_sub_title") or row.get("subtitle"))
                        if row.get("yes_sub_title") or row.get("subtitle")
                        else None
                    ),
                    category=category,
                    created_time=str(row["created_time"]),
                    close_time=(
                        str(row["close_time"]) if row.get("close_time") else None
                    ),
                )
            )
        return MarketFetchResult(
            markets=sorted(
                results, key=lambda item: (item.category, item.title, item.ticker)
            ),
            categories=categories,
        )
