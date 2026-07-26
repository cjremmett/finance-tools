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

MARCUS_SAVINGS_URL = "https://www.marcus.com/us/en/savings/high-yield-savings"
MARCUS_CD_URL = "https://www.marcus.com/us/en/savings/high-yield-cds/cd-rates"


def _page_text(html: str) -> str:
    return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)


def parse_marcus_savings(html: str) -> ScrapeResult:
    text = _page_text(html)
    patterns = [
        r"(\d+(?:\.\d+)?)\s*%\s*APY\s*Online Savings Account",
        r"Online Savings Account\s+(\d+(?:\.\d+)?)\s*%\s*Annual Percentage Yield",
    ]
    matches: list[str] = []
    for pattern in patterns:
        matches.extend(re.findall(pattern, text, flags=re.IGNORECASE))
    rates = {normalize_percent(value) for value in matches}
    if len(rates) != 1:
        raise ScrapeError(
            f"expected one consistent Marcus savings APY, found {sorted(rates)}"
        )
    effective_date = parse_english_date(
        re.search(
            r"Annual Percentage Yield.*?as of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            text,
            flags=re.IGNORECASE,
        ).group(1)
        if re.search(
            r"Annual Percentage Yield.*?as of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            text,
            flags=re.IGNORECASE,
        )
        else text
    )
    return ScrapeResult(
        source="marcus_savings",
        source_effective_date=effective_date,
        observations=[
            RateObservation(
                source="marcus_savings",
                product_key="marcus_online_savings",
                product_name="Marcus Online Savings Account",
                product_type="savings_apy",
                rate_percent=rates.pop(),
                source_url=MARCUS_SAVINGS_URL,
                source_effective_date=effective_date,
            )
        ],
    )


def _term_months(amount: str, unit: str) -> int:
    value = int(amount)
    return value * 12 if unit.lower().startswith("year") else value


def _type_slug(product_type: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", product_type.lower()).strip("_")


def _canonical_cd_type(product_type: str) -> str:
    return {
        "high-yield cd": "High-Yield CD",
        "no-penalty cd": "No-Penalty CD",
        "rate bump cd": "Rate Bump CD",
    }[product_type.lower()]


def parse_marcus_cds(html: str) -> ScrapeResult:
    text = _page_text(html)
    effective_match = re.search(
        r"CD rates as of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    effective_date = (
        parse_english_date(effective_match.group(1)) if effective_match else None
    )
    row_pattern = re.compile(
        r"(\d+)\s*[- ]?\s*(months?|years?)\s+"
        r"(High-Yield CD|No-Penalty CD|Rate Bump CD)\s+"
        r"(\d+(?:\.\d+)?)\s*%\s+\$([\d,]+)\s+(Yes|No)\b",
        flags=re.IGNORECASE,
    )
    observations: dict[str, RateObservation] = {}
    for match in row_pattern.finditer(text):
        months = _term_months(match.group(1), match.group(2))
        product_type = _canonical_cd_type(match.group(3))
        key = f"marcus_cd:{_type_slug(product_type)}:{months}"
        observation = RateObservation(
            source="marcus_cds",
            product_key=key,
            product_name=f"Marcus {months}-Month {product_type}",
            product_type=product_type,
            term_months=months,
            rate_percent=normalize_percent(match.group(4)),
            source_url=MARCUS_CD_URL,
            source_effective_date=effective_date,
            metadata={
                "minimum_deposit_usd": match.group(5).replace(",", ""),
                "early_withdrawal_penalty": match.group(6).lower(),
            },
        )
        existing = observations.get(key)
        if existing and existing != observation:
            raise ScrapeError(f"conflicting Marcus CD rows for {key}")
        observations[key] = observation

    if not observations:
        raise ScrapeError("no Marcus CD rate rows were found")
    if len(observations) < 3:
        raise ScrapeError(
            f"Marcus CD result is unexpectedly small ({len(observations)} products)"
        )
    return ScrapeResult(
        source="marcus_cds",
        source_effective_date=effective_date,
        observations=list(observations.values()),
    )


def scrape_marcus_savings(timeout_seconds: float) -> ScrapeResult:
    return parse_marcus_savings(fetch_text(MARCUS_SAVINGS_URL, timeout_seconds))


def scrape_marcus_cds(timeout_seconds: float) -> ScrapeResult:
    return parse_marcus_cds(fetch_text(MARCUS_CD_URL, timeout_seconds))
