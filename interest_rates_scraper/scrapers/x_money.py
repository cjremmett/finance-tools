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

_PRODUCT_KEY = "x_money_premium_plus_apy"
_PRODUCT_NAME = "X Money Premium+ APY"

_PERCENT_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*%$")
_NA_RE = re.compile(r"^n/?a$", re.IGNORECASE)


def _cell_text(cell: object) -> str:
    return re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()


def _is_premium_plus(text: str) -> bool:
    compact = re.sub(r"\s+", " ", text).strip().lower().replace("premium +", "premium+")
    return "premium+" in compact


def _is_standard_apy_label(text: str) -> bool:
    lowered = text.lower()
    return "standard" in lowered and "annual percentage yield" in lowered


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


def _table_rows(table: object) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in table.find_all("tr"):
        cells = [_cell_text(cell) for cell in row.find_all(["th", "td"])]
        if cells:
            rows.append(cells)
    return rows


def _premium_plus_columns(rows: list[list[str]]) -> set[int]:
    # The rate column is "Premium, Super, Ultra". Its legacy label is "Premium+".
    return {
        index
        for row in rows
        for index, cell in enumerate(row)
        if index > 0 and _is_premium_plus(cell)
    }


def _rates_table(soup: BeautifulSoup) -> tuple[list[list[str]], int]:
    matches: list[tuple[list[list[str]], int]] = []
    for table in soup.find_all("table"):
        rows = _table_rows(table)
        columns = _premium_plus_columns(rows)
        if len(columns) == 1:
            matches.append((rows, next(iter(columns))))
    if not matches:
        raise ScrapeError("X Money rates table was not found")
    if len(matches) > 1:
        raise ScrapeError("multiple X Money rates tables were found")
    return matches[0]


def _premium_plus_apy(rows: list[list[str]], column: int) -> str:
    matched = [row for row in rows if row and _is_standard_apy_label(row[0])]
    if len(matched) != 1:
        raise ScrapeError(
            f"expected one X Money Standard APY row, found {len(matched)}"
        )
    row = matched[0]
    if len(row) <= column:
        raise ScrapeError("X Money Standard APY row is missing the Premium+ rate")
    raw = row[column]
    rate = _parse_apy_cell(raw)
    if rate is None:
        raise ScrapeError(
            f"expected a numeric APY for {_PRODUCT_NAME}, found {raw!r}"
        )
    return rate


def parse_x_money(html: str) -> ScrapeResult:
    soup = BeautifulSoup(html, "html.parser")
    rows, column = _rates_table(soup)
    rate = _premium_plus_apy(rows, column)
    effective_date = _effective_date(soup.get_text(" ", strip=True))
    observation = RateObservation(
        source="x_money",
        product_key=_PRODUCT_KEY,
        product_name=_PRODUCT_NAME,
        product_type="savings_apy",
        rate_percent=rate,
        source_url=X_MONEY_RATES_URL,
        source_effective_date=effective_date,
    )
    return ScrapeResult(
        source="x_money",
        source_effective_date=effective_date,
        observations=[observation],
    )


def scrape_x_money(timeout_seconds: float) -> ScrapeResult:
    return parse_x_money(fetch_text(X_MONEY_RATES_URL, timeout_seconds))
