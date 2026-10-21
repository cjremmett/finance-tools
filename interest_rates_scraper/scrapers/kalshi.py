from __future__ import annotations

import re
from datetime import datetime

from bs4 import BeautifulSoup

from interest_rates_scraper.models import RateObservation, ScrapeResult
from interest_rates_scraper.scrapers.common import (
    ScrapeError,
    fetch_text,
    normalize_percent,
    parse_english_date,
)

KALSHI_APY_URL = "https://help.kalshi.com/en/articles/13823847-apy-on-kalshi"

_HEADING_TAGS = ("h1", "h2", "h3")
_PREDICTION_HEADING = "prediction market interest"
_RATE_RE = re.compile(
    r"^rate:\s*(\d+(?:\.\d+)?)\s*%\s*apy\b",
    flags=re.IGNORECASE,
)


def _heading_text(heading: object) -> str:
    return re.sub(r"\s+", " ", heading.get_text(" ", strip=True)).strip().lower()


def _prediction_section_paragraphs(article: object) -> list[str]:
    headings = [
        heading
        for heading in article.find_all(_HEADING_TAGS)
        if _heading_text(heading) == _PREDICTION_HEADING
    ]
    if len(headings) != 1:
        raise ScrapeError(
            "expected one Kalshi Prediction Market Interest section, "
            f"found {len(headings)}"
        )

    paragraphs: list[str] = []
    for element in headings[0].next_elements:
        name = getattr(element, "name", None)
        if name in _HEADING_TAGS:
            break
        if name != "p":
            continue
        text = re.sub(r"\s+", " ", element.get_text(" ", strip=True)).strip()
        if text:
            paragraphs.append(text)
    return paragraphs


def _prediction_rate(paragraphs: list[str]) -> str:
    matches = [
        match.group(1)
        for paragraph in paragraphs
        if (match := _RATE_RE.match(paragraph))
    ]
    if len(matches) != 1:
        raise ScrapeError(
            f"expected one Kalshi prediction-market APY, found {len(matches)}"
        )
    return normalize_percent(matches[0])


def _iso_date(value: str) -> str | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def _source_effective_date(soup: BeautifulSoup) -> str | None:
    updated: list[str] = []
    dated: list[str] = []
    for time in soup.find_all("time"):
        parsed = _iso_date((time.get("datetime") or "").strip())
        if parsed is None:
            continue
        dated.append(parsed)
        if time.get_text(" ", strip=True).lower().startswith("updated"):
            updated.append(parsed)
    if len(dict.fromkeys(updated)) == 1:
        return updated[0]
    unique = list(dict.fromkeys(dated))
    if len(unique) == 1:
        return unique[0]
    return parse_english_date(soup.get_text(" ", strip=True))


def parse_kalshi(html: str) -> ScrapeResult:
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find("article") or soup
    rate = _prediction_rate(_prediction_section_paragraphs(article))
    effective_date = _source_effective_date(soup)
    return ScrapeResult(
        source="kalshi",
        source_effective_date=effective_date,
        observations=[
            RateObservation(
                source="kalshi",
                product_key="kalshi_predictions_apy",
                product_name="Kalshi Cash and Open Positions APY",
                product_type="prediction_account_apy",
                rate_percent=rate,
                source_url=KALSHI_APY_URL,
                source_effective_date=effective_date,
            )
        ],
    )


def scrape_kalshi(timeout_seconds: float) -> ScrapeResult:
    return parse_kalshi(fetch_text(KALSHI_APY_URL, timeout_seconds))
