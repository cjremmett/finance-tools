from __future__ import annotations

import csv
import io
from datetime import date, timedelta

from interest_rates_scraper.models import RateObservation, ScrapeResult
from interest_rates_scraper.scrapers.common import ScrapeError, fetch_text, normalize_percent

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


def _series_url(series_id: str) -> str:
    # FRED returns the entire daily history by default. A recent window is enough
    # for a current rate and keeps this activity fast and bandwidth-friendly.
    start_date = date.today() - timedelta(days=45)
    return f"{FRED_CSV_URL}?id={series_id}&cosd={start_date.isoformat()}"


def _latest_value(csv_text: str, series_id: str) -> tuple[str, str]:
    rows = csv.DictReader(io.StringIO(csv_text))
    latest: tuple[str, str] | None = None
    for row in rows:
        value = row.get(series_id)
        date = row.get("DATE") or row.get("observation_date")
        if date and value and value != ".":
            latest = (date, normalize_percent(value))
    if latest is None:
        raise ScrapeError(f"no usable observations in FRED series {series_id}")
    return latest


def scrape_fed(timeout_seconds: float) -> ScrapeResult:
    lower_url = _series_url("DFEDTARL")
    upper_url = _series_url("DFEDTARU")
    lower_date, lower = _latest_value(
        fetch_text(lower_url, timeout_seconds), "DFEDTARL"
    )
    upper_date, upper = _latest_value(
        fetch_text(upper_url, timeout_seconds), "DFEDTARU"
    )
    if lower_date != upper_date:
        raise ScrapeError(
            f"FRED target bounds have different effective dates: {lower_date}, {upper_date}"
        )

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
