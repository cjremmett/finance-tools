from __future__ import annotations

import time
from collections.abc import Callable

import requests

from marketparquet_etl.models import API_BASE, API_PAGE_CAP, require_session_date

_DATASETS = {"stock_daily", "etf_daily"}

_RETRY_LIMIT = 5
_DEFAULT_RETRY_AFTER = 60.0
_MAX_RETRY_AFTER = 120.0


def retry_after_seconds(headers: object) -> float:
    raw = ""
    getter = getattr(headers, "get", None)
    if callable(getter):
        value = getter("Retry-After", "")
        raw = "" if value is None else str(value).strip()
    if not raw:
        return _DEFAULT_RETRY_AFTER
    try:
        seconds = float(raw)
    except ValueError:
        return _DEFAULT_RETRY_AFTER
    if seconds < 0:
        return _DEFAULT_RETRY_AFTER
    return min(seconds, _MAX_RETRY_AFTER)


class MarketParquetClient:
    def __init__(
        self,
        api_key: str,
        limiter: object,
        session: requests.Session | None = None,
        sleeper: Callable[[float], None] | None = None,
        heartbeat: Callable[..., None] | None = None,
        timeout: float = 60,
    ) -> None:
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self._limiter = limiter
        self._session = session or requests.Session()
        self._sleeper = sleeper or time.sleep
        self._heartbeat = heartbeat
        self.timeout = timeout

    def list_dates(self, asset_type: str = "stock_daily") -> list[str]:
        dataset = _dataset(asset_type)
        found: list[str] = []
        for page in range(1, API_PAGE_CAP + 1):
            body = self._api_get(f"/dates/{dataset}", {"page": page})
            self._beat(f"dates:{page}")
            items = body.get("dates")
            if not isinstance(items, list) or not items:
                break
            for item in items:
                if isinstance(item, dict) and isinstance(item.get("date"), str):
                    found.append(require_session_date(str(item["date"])))
            per_page = body.get("per_page")
            if not isinstance(per_page, int) or per_page < 1:
                per_page = 100
            if len(items) < per_page:
                break
        else:
            raise RuntimeError(
                f"MarketParquet date list exceeded {API_PAGE_CAP} pages"
            )
        return sorted(set(found))

    def manifest(
        self, start: str, end: str, asset_type: str = "stock_daily"
    ) -> dict[str, str]:
        dataset = _dataset(asset_type)
        body = self._api_get(
            f"/manifest/{dataset}",
            {
                "start": require_session_date(start),
                "end": require_session_date(end),
            },
        )
        files = body.get("files")
        if not isinstance(files, list):
            raise RuntimeError("MarketParquet manifest has no files")
        found: dict[str, str] = {}
        for item in files:
            if not isinstance(item, dict):
                continue
            day = item.get("date")
            url = item.get("download_url")
            if (
                isinstance(day, str)
                and isinstance(url, str)
                and url.startswith("https://")
            ):
                found[require_session_date(day)] = url
        return found

    def download_day(self, day: str, asset_type: str = "stock_daily") -> bytes:
        dataset = _dataset(asset_type)
        body = self._api_get(
            f"/download/{dataset}/{require_session_date(day)}"
        )
        url = body.get("download_url")
        if not isinstance(url, str) or not url.startswith("https://"):
            raise RuntimeError("MarketParquet download response has no URL")
        return self.get_bytes(url)

    def get_bytes(self, url: str) -> bytes:
        response = self._session.get(url, timeout=max(self.timeout, 120))
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"download failed with HTTP {response.status_code}"
            )
        return response.content

    def _api_get(self, path: str, params: dict[str, object] | None = None) -> dict:
        url = f"{API_BASE}{path}"
        for _attempt in range(_RETRY_LIMIT):
            self._limiter.acquire(self._sleep)  # type: ignore[attr-defined]
            response = self._session.get(
                url,
                headers=self._headers,
                params=params,
                timeout=self.timeout,
            )
            if response.status_code == 429:
                self._sleep(retry_after_seconds(response.headers))
                continue
            if response.status_code >= 400:
                raise RuntimeError(
                    f"MarketParquet {path} returned HTTP {response.status_code}"
                )
            body = response.json()
            if not isinstance(body, dict):
                raise RuntimeError(f"MarketParquet {path} returned a non-object")
            return body
        raise RuntimeError(f"MarketParquet {path} stayed rate limited")

    def _sleep(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0:
            step = min(10.0, remaining)
            self._sleeper(step)
            remaining -= step
            self._beat("rate-limit")

    def _beat(self, detail: str) -> None:
        if self._heartbeat is not None:
            self._heartbeat(detail)


def _dataset(asset_type: str) -> str:
    if asset_type not in _DATASETS:
        raise ValueError(f"unsupported MarketParquet dataset: {asset_type}")
    return asset_type
