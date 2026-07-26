from __future__ import annotations

from typing import Any

import requests

from interest_rates_scraper.models import RateObservation, ScrapeResult
from interest_rates_scraper.scrapers.common import (
    USER_AGENT,
    ScrapeError,
    normalize_percent,
)

FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES_PAGE = "https://fred.stlouisfed.org/series"


def _latest_value(payload: dict[str, Any], series_id: str) -> tuple[str, str]:
    observations = payload.get("observations")
    if not isinstance(observations, list):
        raise ScrapeError(f"invalid FRED response for series {series_id}")

    for observation in observations:
        if not isinstance(observation, dict):
            continue
        observation_date = observation.get("date")
        value = observation.get("value")
        if (
            isinstance(observation_date, str)
            and isinstance(value, str)
            and value != "."
        ):
            return observation_date, normalize_percent(value)

    raise ScrapeError(f"no usable observations in FRED series {series_id}")


def _fetch_latest(
    series_id: str, api_key: str, timeout_seconds: float
) -> tuple[str, str]:
    try:
        response = requests.get(
            FRED_API_URL,
            params={
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 10,
            },
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        # Do not include the exception text: Requests includes the full URL,
        # which contains the FRED API key, in several error messages.
        raise ScrapeError(
            f"FRED API request failed for series {series_id} "
            f"({type(exc).__name__})"
        ) from exc

    if not isinstance(payload, dict):
        raise ScrapeError(f"invalid FRED response for series {series_id}")
    return _latest_value(payload, series_id)


def scrape_fed(timeout_seconds: float, api_key: str | None) -> ScrapeResult:
    if not api_key:
        raise ScrapeError("FRED_API_KEY is required")

    lower_date, lower = _fetch_latest("DFEDTARL", api_key, timeout_seconds)
    upper_date, upper = _fetch_latest("DFEDTARU", api_key, timeout_seconds)
    if lower_date != upper_date:
        raise ScrapeError(
            f"FRED target bounds have different effective dates: {lower_date}, {upper_date}"
        )

    lower_url = f"{FRED_SERIES_PAGE}/DFEDTARL"
    upper_url = f"{FRED_SERIES_PAGE}/DFEDTARU"
    return ScrapeResult(
        source="federal_reserve",
        source_effective_date=lower_date,
        observations=[
            RateObservation(
                source="federal_reserve",
                product_key="fed_funds_target_lower",
                product_name="Federal Funds Target Lower Bound",
                product_type="target_lower_bound",
                rate_percent=lower,
                source_url=lower_url,
                source_effective_date=lower_date,
            ),
            RateObservation(
                source="federal_reserve",
                product_key="fed_funds_target_upper",
                product_name="Federal Funds Target Upper Bound",
                product_type="target_upper_bound",
                rate_percent=upper,
                source_url=upper_url,
                source_effective_date=upper_date,
            ),
        ],
    )
