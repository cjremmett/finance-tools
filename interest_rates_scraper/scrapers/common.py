from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import requests

USER_AGENT = "finance-tools-interest-rate-monitor/0.1 (+personal monitoring)"


class ScrapeError(RuntimeError):
    """Raised when a source cannot be fetched or parsed safely."""


def fetch_text(url: str, timeout_seconds: float) -> str:
    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,text/csv;q=0.9,*/*;q=0.8",
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise ScrapeError(f"request failed for {url}: {exc}") from exc

    if not response.text.strip():
        raise ScrapeError(f"empty response from {url}")
    return response.text


def normalize_percent(value: str) -> str:
    try:
        result = Decimal(value.replace(",", "").strip())
    except InvalidOperation as exc:
        raise ScrapeError(f"invalid percentage: {value!r}") from exc
    if result < 0 or result > 100:
        raise ScrapeError(f"percentage outside expected range: {value!r}")
    return format(result.normalize(), "f")


def parse_english_date(text: str) -> str | None:
    match = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+(\d{1,2}),\s+(\d{4})\b",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return datetime.strptime(match.group(0), "%B %d, %Y").date().isoformat()
