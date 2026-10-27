from __future__ import annotations

import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

DATASETS = ("stocks", "option_underlyings")
FETCH_METHODS = {
    "stocks": "stock_list_symbols",
    "option_underlyings": "option_list_symbols",
}
MIN_SYMBOL_ROWS = 10_000
BUCKET = "securities-data"
WAREHOUSE = "securities_data"
NAMESPACE = "securities"
KEY_PREFIX = "securities"
EASTERN = ZoneInfo("America/New_York")

RESOLVE_SYMBOL_FETCH_DATE = "resolve_symbol_fetch_date"
SYMBOL_CSV_EXISTS = "symbol_csv_exists"
FETCH_AND_UPLOAD_SYMBOLS = "fetch_and_upload_symbols"
ENSURE_SECURITIES_CATALOG = "ensure_securities_catalog"
APPEND_SYMBOLS_TO_ICEBERG = "append_symbols_to_iceberg"

_CATALOG_URI = re.compile(r"https?://[A-Za-z0-9._:-]+(?:/[A-Za-z0-9._~/-]*)?")


def require_dataset(dataset: str) -> str:
    if dataset not in DATASETS:
        raise ValueError(f"unknown symbol dataset: {dataset}")
    return dataset


def require_fetch_date(fetch_date: str) -> str:
    try:
        parsed = date.fromisoformat(fetch_date)
    except ValueError as error:
        raise ValueError(f"fetch date must be YYYY-MM-DD: {fetch_date}") from error
    if parsed.isoformat() != fetch_date:
        raise ValueError(f"fetch date must be YYYY-MM-DD: {fetch_date}")
    return fetch_date


def require_catalog_uri(catalog_uri: str) -> str:
    if _CATALOG_URI.fullmatch(catalog_uri) is None:
        raise ValueError(f"invalid Iceberg catalog URI: {catalog_uri}")
    return catalog_uri


def object_key(dataset: str, fetch_date: str) -> str:
    return (
        f"{KEY_PREFIX}/{require_dataset(dataset)}/"
        f"{require_fetch_date(fetch_date)}.csv"
    )


def qualified_table(dataset: str) -> str:
    return f"{WAREHOUSE}.{NAMESPACE}.{require_dataset(dataset)}"


def eastern_fetch_date(observed_at: str) -> str:
    moment = datetime.fromisoformat(observed_at)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=ZoneInfo("UTC"))
    return moment.astimezone(EASTERN).date().isoformat()
