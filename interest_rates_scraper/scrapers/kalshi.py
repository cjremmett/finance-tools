from __future__ import annotations

import re

from bs4 import BeautifulSoup

from interest_rates_scraper.models import RateObservation, ScrapeResult
from interest_rates_scraper.scrapers.common import (
    ScrapeError,
    fetch_text,
    normalize_percent,
    parse_english_date,
)

KALSHI_APY_URL = "https://help.kalshi.com/en/articles/13823847-apy-on-kalshi"


def parse_kalshi(html: str) -> ScrapeResult:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    match = re.search(
        r"interest rate is set at\s+(\d+(?:\.\d+)?)\s*%",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        raise ScrapeError("Kalshi APY sentence was not found")
    effective_date = parse_english_date(text)
    return ScrapeResult(
        source="kalshi",
        source_effective_date=effective_date,
        observations=[
            RateObservation(
                source="kalshi",
                product_key="kalshi_predictions_apy",
                product_name="Kalshi Cash and Open Positions APY",
                product_type="prediction_account_apy",
                rate_percent=normalize_percent(match.group(1)),
                source_url=KALSHI_APY_URL,
                source_effective_date=effective_date,
            )
        ],
    )


def scrape_kalshi(timeout_seconds: float) -> ScrapeResult:
    return parse_kalshi(fetch_text(KALSHI_APY_URL, timeout_seconds))
