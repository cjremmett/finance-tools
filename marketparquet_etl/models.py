from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

BUCKET = "securities-data"
WAREHOUSE = "securities_data"
NAMESPACE = "securities"
TABLE = "stock_data"
KEY_PREFIX = "securities/stock_data"
API_BASE = "https://marketparquet.com/api/v1"
FETCH_BATCH = 40
APPEND_BATCH = 25
MANIFEST_LIMIT = 400
API_PAGE_CAP = 20
COLUMNS = (
    "date",
    "symbol",
    "asset_type",
    "open",
    "high",
    "low",
    "close",
    "volume",
)
CSV_TYPES = {
    "date": "DATE",
    "symbol": "VARCHAR",
    "asset_type": "VARCHAR",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "volume": "DOUBLE",
}

ENSURE_MARKETPARQUET_CATALOG = "ensure_marketparquet_catalog"
PLAN_MARKETPARQUET_STOCK_SYNC = "plan_marketparquet_stock_sync"
FETCH_MARKETPARQUET_STOCK_DAYS = "fetch_marketparquet_stock_days"
APPEND_MARKETPARQUET_STOCK_DAYS = "append_marketparquet_stock_days"
SEND_MARKETPARQUET_DISCORD = "send_marketparquet_discord_message"
WORKFLOW_NAME = "MarketParquetStockDataWorkflow"

STOCKS = "stocks"
ETFS = "etfs"
# ETF daily files in the free window are about 4,000 rows, so the stock
# floor of 5,000 would reject a complete ETF session.
MIN_ROWS = 5_000
ETF_MIN_ROWS = 3_000

_SESSION_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CSV_NAME = re.compile(r"^(?:(stocks|etfs)-)?(\d{4}-\d{2}-\d{2})\.csv$")
QUALIFIED_TABLE = f"{WAREHOUSE}.{NAMESPACE}.{TABLE}"


@dataclass(frozen=True)
class AssetSpec:
    name: str
    api_type: str
    label: str
    file_prefix: str
    min_rows: int
    display: str


SPECS: dict[str, AssetSpec] = {
    STOCKS: AssetSpec(
        STOCKS, "stock_daily", "Stock", "stocks-", MIN_ROWS, "Stocks"
    ),
    ETFS: AssetSpec(ETFS, "etf_daily", "ETF", "etfs-", ETF_MIN_ROWS, "ETFs"),
}


def require_asset(name: str) -> AssetSpec:
    spec = SPECS.get(name)
    if spec is None:
        raise ValueError(f"asset must be stocks or etfs: {name}")
    return spec


def require_session_date(value: str) -> str:
    if _SESSION_DATE.fullmatch(value) is None:
        raise ValueError(f"session date must be YYYY-MM-DD: {value}")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"session date must be YYYY-MM-DD: {value}") from error
    if parsed.isoformat() != value:
        raise ValueError(f"session date must be YYYY-MM-DD: {value}")
    return value


def object_key(asset: str, day: str) -> str:
    spec = require_asset(asset)
    return f"{KEY_PREFIX}/{spec.file_prefix}{require_session_date(day)}.csv"


def legacy_stock_key(day: str) -> str:
    """Stock CSVs written before the stocks-/etfs- prefixes."""
    return f"{KEY_PREFIX}/{require_session_date(day)}.csv"


def require_csv_key(asset: str, key: str) -> str:
    spec = require_asset(asset)
    prefix = f"{KEY_PREFIX}/"
    if not key.startswith(prefix):
        raise ValueError(f"unexpected CSV key for {spec.name}")
    match = _CSV_NAME.fullmatch(key[len(prefix) :])
    if match is None:
        raise ValueError(f"unexpected CSV key for {spec.name}")
    kind, day = match.group(1), require_session_date(match.group(2))
    if kind is None:
        if spec.name != STOCKS:
            raise ValueError(f"unexpected CSV key for {spec.name}")
        return legacy_stock_key(day)
    if kind != spec.name:
        raise ValueError(f"unexpected CSV key for {spec.name}")
    return object_key(spec.name, day)


def window_bounds(dates: list[str]) -> tuple[str, str]:
    if not dates:
        return "", ""
    ordered = sorted(dates)
    return ordered[0], ordered[-1]


def batches_of(dates: list[str], size: int) -> list[list[str]]:
    if size < 1:
        raise ValueError("batch size must be positive")
    return [dates[index : index + size] for index in range(0, len(dates), size)]


def contiguous_missing(
    published: list[str], present: set[str], limit: int
) -> list[list[str]]:
    if limit < 1:
        raise ValueError("range limit must be positive")
    current: list[str] = []
    ranges: list[list[str]] = []
    for day in sorted(set(published)):
        if day in present:
            if current:
                ranges.append(current)
                current = []
            continue
        current.append(day)
        if len(current) == limit:
            ranges.append(current)
            current = []
    if current:
        ranges.append(current)
    return ranges


def fetch_batches(published: list[str], csv_dates: list[str]) -> list[list[str]]:
    ranges = contiguous_missing(published, set(csv_dates), MANIFEST_LIMIT)
    chunks: list[list[str]] = []
    for item in ranges:
        chunks.extend(batches_of(item, FETCH_BATCH))
    return chunks


def append_batches(published: list[str], iceberg_dates: list[str]) -> list[list[str]]:
    loaded = set(iceberg_dates)
    missing = [day for day in sorted(set(published)) if day not in loaded]
    return batches_of(missing, APPEND_BATCH)


@dataclass(frozen=True)
class AssetState:
    asset: str
    dates: list[str]
    csv_dates: list[str]
    iceberg_dates: list[str]


@dataclass(frozen=True)
class AssetChunk:
    asset: str
    dates: list[str]


@dataclass(frozen=True)
class StockPlan:
    stocks: AssetState
    etfs: AssetState


@dataclass(frozen=True)
class FetchChunkResult:
    dates: list[str]
    row_counts: list[int]


@dataclass(frozen=True)
class AppendChunkResult:
    appended_days: int
    appended_rows: int
    already_stored: int


@dataclass(frozen=True)
class WorkflowResult:
    workflow_run_id: str
    window_start: str
    window_end: str
    session_count: int
    downloaded: int
    reused: int
    appended_days: int
    appended_rows: int
    already_stored: int
    idle: bool
    stock_sessions: int
    etf_sessions: int
    stock_downloaded: int
    etf_downloaded: int
    stock_appended_days: int
    etf_appended_days: int
    stock_appended_rows: int
    etf_appended_rows: int
