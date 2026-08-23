from __future__ import annotations

from typing import Any

import requests

USER_AGENT = "finance-tools-crypto-carry/0.1 (+personal monitoring)"


class ScrapeError(RuntimeError):
    """Raised when a source cannot be fetched or parsed safely."""


def fetch_json(
    method: str,
    url: str,
    timeout_seconds: float,
    *,
    payload: dict[str, Any] | None = None,
) -> Any:
    try:
        response = requests.request(
            method,
            url,
            json=payload,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise ScrapeError(f"request failed for {url}: {exc}") from exc

    if not response.content.strip():
        raise ScrapeError(f"empty response from {url}")
    try:
        return response.json()
    except ValueError as exc:
        raise ScrapeError(f"invalid JSON from {url}") from exc
