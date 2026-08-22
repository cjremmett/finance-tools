from __future__ import annotations

import re
from datetime import date

from bs4 import BeautifulSoup

from interest_rates_scraper.models import RateObservation, ScrapeResult
from interest_rates_scraper.scrapers.common import (
    ScrapeError,
    fetch_text,
    normalize_percent,
)

X_MONEY_RATES_URL = "https://money.x.com/en/i/interest-faq"

_TIER_PREMIUM = "premium"
_TIER_PREMIUM_PLUS = "premium_plus"
_BAND_STANDARD = "standard"
_BAND_BOOSTED = "boosted"

_TRACKED = {
    (_BAND_BOOSTED, _TIER_PREMIUM): (
        "x_money_boosted_apy",
        "X Money Boosted APY",
    ),
    (_BAND_STANDARD, _TIER_PREMIUM_PLUS): (
        "x_money_premium_plus_apy",
        "X Money Premium+ APY",
    ),
}
_PRODUCT_ORDER = ("x_money_boosted_apy", "x_money_premium_plus_apy")

_PERCENT_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*%$")
_NA_RE = re.compile(r"^n/?a$", re.IGNORECASE)


def _cell_text(cell: object) -> str:
    return re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()


def _header_tier(text: str) -> str | None:
    compact = re.sub(r"\s+", " ", text).strip().lower().replace("premium +", "premium+")
    if "premium+" in compact:
        return _TIER_PREMIUM_PLUS
    if "premium" in compact:
        return _TIER_PREMIUM
    return None


def _band(text: str) -> str | None:
    lowered = text.lower().lstrip()
    if lowered.startswith("boosted"):
        return _BAND_BOOSTED
    if lowered.startswith("standard"):
        return _BAND_STANDARD
    return None


def _is_apy_row(cells: list[str]) -> bool:
    metric_cells = cells[:-2] if len(cells) >= 2 else cells
    return any("annual percentage yield" in cell.lower() for cell in metric_cells)


def _parse_apy_cell(value: str) -> str | None:
    cleaned = value.strip()
    if _NA_RE.fullmatch(cleaned):
        return None
    match = _PERCENT_RE.fullmatch(cleaned)
    if not match:
        raise ScrapeError(f"invalid X Money APY cell: {value!r}")
    return normalize_percent(match.group(1))


def _effective_date(text: str) -> str | None:
    match = re.search(
        r"effective as of\s+(\d{1,2})/(\d{1,2})/(\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    try:
        return date(
            int(match.group(3)), int(match.group(1)), int(match.group(2))
        ).isoformat()
    except ValueError as exc:
        raise ScrapeError(
            f"invalid X Money effective date: {match.group(1)}/{match.group(2)}/{match.group(3)}"
        ) from exc


def _tier_columns(header: list[str]) -> list[str]:
    if len(header) < 2:
        raise ScrapeError("X Money rates table header is missing tier columns")
    trailing = [_header_tier(cell) for cell in header[-2:]]
    if set(trailing) != {_TIER_PREMIUM, _TIER_PREMIUM_PLUS}:
        raise ScrapeError(
            f"expected X Premium and X Premium+ headers, found {header[-2:]}"
        )
    return trailing


def _rates_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        header = [_cell_text(cell) for cell in rows[0].find_all(["th", "td"])]
        tiers = {_header_tier(cell) for cell in header}
        if {_TIER_PREMIUM, _TIER_PREMIUM_PLUS} <= tiers:
            return table
    raise ScrapeError("X Money rates table was not found")


def parse_x_money(html: str) -> ScrapeResult:
    soup = BeautifulSoup(html, "html.parser")
    table = _rates_table(soup)
    rows = table.find_all("tr")
    tier_order = _tier_columns(
        [_cell_text(cell) for cell in rows[0].find_all(["th", "td"])]
    )
    effective_date = _effective_date(soup.get_text(" ", strip=True))

    current_band: str | None = None
    observations: dict[str, RateObservation] = {}
    for row in rows[1:]:
        cells = [_cell_text(cell) for cell in row.find_all(["th", "td"])]
        if not cells:
            continue
        maybe_band = _band(cells[0])
        if maybe_band is not None:
            current_band = maybe_band
        if not _is_apy_row(cells):
            continue
        if current_band is None:
            raise ScrapeError("X Money APY row is missing a Standard/Boosted label")
        if len(cells) < 3:
            raise ScrapeError("X Money APY row is missing rate columns")
        for tier, raw in zip(tier_order, cells[-2:], strict=True):
            spec = _TRACKED.get((current_band, tier))
            if spec is None:
                continue
            product_key, product_name = spec
            rate = _parse_apy_cell(raw)
            if rate is None:
                raise ScrapeError(
                    f"expected a numeric APY for {product_name}, found {raw!r}"
                )
            observation = RateObservation(
                source="x_money",
                product_key=product_key,
                product_name=product_name,
                product_type="savings_apy",
                rate_percent=rate,
                source_url=X_MONEY_RATES_URL,
                source_effective_date=effective_date,
            )
            existing = observations.get(product_key)
            if existing is not None and existing != observation:
                raise ScrapeError(f"conflicting X Money rows for {product_key}")
            observations[product_key] = observation

    expected = set(_PRODUCT_ORDER)
    if observations.keys() != expected:
        raise ScrapeError(
            "expected X Money Boosted and Premium+ APYs, found "
            f"{sorted(observations)}"
        )
    return ScrapeResult(
        source="x_money",
        source_effective_date=effective_date,
        observations=[observations[key] for key in _PRODUCT_ORDER],
    )


def scrape_x_money(timeout_seconds: float) -> ScrapeResult:
    return parse_x_money(fetch_text(X_MONEY_RATES_URL, timeout_seconds))
