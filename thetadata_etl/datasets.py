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
RESOLVE_OPTION_SESSION = "resolve_option_session"
OPTION_DAY_LOADED = "option_day_loaded"
CONFIRM_OPTION_OPEN_INTEREST = "confirm_option_open_interest"
PLAN_OPTION_BATCHES = "plan_option_batches"
FETCH_OPTION_BATCH = "fetch_option_batch"
APPEND_OPTION_BATCH = "append_option_batch"
COMPLETE_OPTION_DAY = "complete_option_day"
PLAN_OPTION_BACKFILL = "plan_option_backfill"
FETCH_OPTION_BACKFILL_SYMBOL = "fetch_option_backfill_symbol"
APPEND_OPTION_BACKFILL_BATCH = "append_option_backfill_batch"
SEND_THETADATA_DISCORD = "send_thetadata_discord_message"

SECURITIES_WORKFLOW = "ThetaDataSecuritiesWorkflow"
OPTION_DATA_WORKFLOW = "ThetaDataOptionDataWorkflow"
OPTION_BACKFILL_WORKFLOW = "ThetaDataOptionBackfillWorkflow"
BACKFILL_SYMBOL_FAILED = "BackfillSymbolFailed"
BACKFILL_CHUNK_SESSIONS = 5
BACKFILL_DATA_ATTEMPTS = 5
OPTION_OI_PROBE_SYMBOL = "AAPL"
OPEN_INTEREST_NOT_READY = "OpenInterestNotReady"
OPTION_DATA_TABLE = f"{WAREHOUSE}.{NAMESPACE}.option_data"
OPTION_VOLUME_TABLE = f"{WAREHOUSE}.{NAMESPACE}.option_symbol_volume"

_CATALOG_URI = re.compile(r"https?://[A-Za-z0-9._:-]+(?:/[A-Za-z0-9._~/-]*)?")
_OPTION_SYMBOL = re.compile(r"^[A-Za-z0-9._-]+$")
_PART_KEY = re.compile(
    r"^securities/option_data/\d{4}-\d{2}-\d{2}/part-\d{4}\.csv$"
)
_BACKFILL_PART_KEY = re.compile(
    r"^securities/option_backfill/parts/\d{4}-\d{2}-\d{2}/part-\d{4}\.csv$"
)
_BACKFILL_MARKER_KEY = re.compile(
    r"^securities/option_backfill/(?P<symbol>[A-Za-z0-9._-]+)\.json$"
)


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


def require_option_symbol(symbol: str) -> str:
    if _OPTION_SYMBOL.fullmatch(symbol) is None:
        raise ValueError(f"invalid option symbol: {symbol}")
    return symbol


def require_option_part_key(key: str) -> str:
    if _PART_KEY.fullmatch(key) is None:
        raise ValueError(f"invalid option part key: {key}")
    return key


def option_prefix(trade_date: str) -> str:
    return f"{KEY_PREFIX}/option_data/{require_fetch_date(trade_date)}"


def option_symbol_key(trade_date: str, symbol: str) -> str:
    return (
        f"{option_prefix(trade_date)}/symbols/{require_option_symbol(symbol)}.csv"
    )


def option_part_key(trade_date: str, batch_index: int) -> str:
    if batch_index < 0:
        raise ValueError("batch index must be >= 0")
    return f"{option_prefix(trade_date)}/part-{batch_index:04d}.csv"


def option_complete_key(trade_date: str) -> str:
    return f"{option_prefix(trade_date)}/_COMPLETE"


def backfill_marker_key(symbol: str) -> str:
    return f"{KEY_PREFIX}/option_backfill/{require_option_symbol(symbol)}.json"


def backfill_marker_symbol(key: str) -> str | None:
    match = _BACKFILL_MARKER_KEY.fullmatch(key)
    if match is None:
        return None
    return match.group("symbol")


def backfill_part_key(window_end: str, batch_index: int) -> str:
    if batch_index < 0:
        raise ValueError("batch index must be >= 0")
    return (
        f"{KEY_PREFIX}/option_backfill/parts/"
        f"{require_fetch_date(window_end)}/part-{batch_index:04d}.csv"
    )


def require_backfill_part_key(key: str) -> str:
    if _BACKFILL_PART_KEY.fullmatch(key) is None:
        raise ValueError(f"invalid backfill part key: {key}")
    return key


def snapshot_dates(keys: list[str], prefix: str) -> set[str]:
    dates: set[str] = set()
    for key in keys:
        if not key.startswith(prefix):
            continue
        name = key[len(prefix) :]
        if "/" in name or not name.endswith(".csv"):
            continue
        day = name[: -len(".csv")]
        try:
            require_fetch_date(day)
        except ValueError:
            continue
        dates.add(day)
    return dates


def latest_shared_snapshot(stock_keys: list[str], option_keys: list[str]) -> str:
    shared = snapshot_dates(stock_keys, f"{KEY_PREFIX}/stocks/") & snapshot_dates(
        option_keys, f"{KEY_PREFIX}/option_underlyings/"
    )
    if not shared:
        raise RuntimeError(
            "no dated symbol snapshot is present for both stocks and option underlyings"
        )
    return max(shared)
