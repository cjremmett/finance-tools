from __future__ import annotations

from datetime import date

from temporalio import activity

from thetadata_etl.catalog import append_symbol_csv, ensure_securities_catalog
from thetadata_etl.config import Settings
from thetadata_etl.datasets import (
    APPEND_SYMBOLS_TO_ICEBERG,
    ENSURE_SECURITIES_CATALOG,
    FETCH_AND_UPLOAD_SYMBOLS,
    RESOLVE_SYMBOL_FETCH_DATE,
    SYMBOL_CSV_EXISTS,
    eastern_fetch_date,
    object_key,
)
from thetadata_etl.models import AppendResult, SymbolSnapshotRequest
from thetadata_etl.storage import garage_store
from thetadata_etl.symbols import build_symbol_csv, fetch_symbol_frame


@activity.defn(name=RESOLVE_SYMBOL_FETCH_DATE)
def resolve_symbol_fetch_date_activity(observed_at: str) -> str:
    return eastern_fetch_date(observed_at)


@activity.defn(name=ENSURE_SECURITIES_CATALOG)
def ensure_securities_catalog_activity() -> None:
    ensure_securities_catalog(Settings.from_env())
    activity.logger.info("securities_data.securities catalog is ready")


@activity.defn(name=SYMBOL_CSV_EXISTS)
def symbol_csv_exists_activity(request: SymbolSnapshotRequest) -> bool:
    key = object_key(request.dataset, request.fetch_date)
    found = garage_store(Settings.from_env()).exists(key)
    activity.logger.info("Garage object %s exists=%s", key, found)
    return found


@activity.defn(name=FETCH_AND_UPLOAD_SYMBOLS)
def fetch_and_upload_symbols_activity(request: SymbolSnapshotRequest) -> int:
    settings = Settings.from_env()
    frame = fetch_symbol_frame(request.dataset, settings.thetadata_api_key)
    body, row_count = build_symbol_csv(
        frame, date.fromisoformat(request.fetch_date)
    )
    key = object_key(request.dataset, request.fetch_date)
    garage_store(settings).put_csv(key, body)
    activity.logger.info(
        "uploaded %s rows=%s bytes=%s", key, row_count, len(body)
    )
    return row_count


@activity.defn(name=APPEND_SYMBOLS_TO_ICEBERG)
def append_symbols_to_iceberg_activity(
    request: SymbolSnapshotRequest,
) -> AppendResult:
    result = append_symbol_csv(
        Settings.from_env(), request.dataset, request.fetch_date
    )
    activity.logger.info(
        "iceberg %s date=%s appended=%s rows=%s",
        request.dataset,
        request.fetch_date,
        result.appended,
        result.row_count,
    )
    return result
