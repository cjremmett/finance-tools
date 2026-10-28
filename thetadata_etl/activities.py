from __future__ import annotations

import io
import json
import logging
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import date

import polars as pl
from temporalio import activity
from temporalio.exceptions import ApplicationError

from thetadata_etl.calendar import resolve_option_session
from thetadata_etl.catalog import (
    append_option_csv,
    append_symbol_csv,
    ensure_securities_catalog,
    latest_option_volumes,
    option_session_row_count,
)
from thetadata_etl.config import Settings
from thetadata_etl.datasets import (
    APPEND_OPTION_BATCH,
    APPEND_SYMBOLS_TO_ICEBERG,
    COMPLETE_OPTION_DAY,
    CONFIRM_OPTION_OPEN_INTEREST,
    ENSURE_SECURITIES_CATALOG,
    FETCH_AND_UPLOAD_SYMBOLS,
    FETCH_OPTION_BATCH,
    KEY_PREFIX,
    OPEN_INTEREST_NOT_READY,
    OPTION_DATA_WORKFLOW,
    OPTION_DAY_LOADED,
    PLAN_OPTION_BATCHES,
    RESOLVE_OPTION_SESSION,
    RESOLVE_SYMBOL_FETCH_DATE,
    SYMBOL_CSV_EXISTS,
    eastern_fetch_date,
    object_key,
    option_complete_key,
    option_part_key,
    option_symbol_key,
    require_fetch_date,
    require_option_symbol,
)
from thetadata_etl.limiter import RedisGate, redis_gate
from thetadata_etl.models import (
    AppendResult,
    BatchAppendResult,
    BatchFetchResult,
    LoadedDay,
    OptionBatch,
    OptionPlan,
    SessionResolution,
    SymbolSnapshotRequest,
)
from thetadata_etl.options import (
    chunk_symbols,
    concat_checkpoint_csvs,
    frame_to_csv,
    intersect_symbols,
    load_option_chain,
    open_interest_covers_session,
    open_interest_probe_symbol,
    order_underlyings,
    request_open_interest,
)
from thetadata_etl.storage import GarageStore, garage_store
from thetadata_etl.symbols import build_symbol_csv, fetch_symbol_frame


@contextmanager
def _thetadata_gate(settings: Settings):
    gate = redis_gate(settings)
    try:
        yield gate
    finally:
        gate.close()


def _theta_client(api_key: str):
    logging.getLogger("thetadata").setLevel(logging.ERROR)
    from thetadata import ThetaClient

    return ThetaClient(api_key=api_key, dataframe_type="polars")


def _calendar_type(frame: object, day: date) -> str:
    if (
        not isinstance(frame, pl.DataFrame)
        or frame.height == 0
        or "type" not in frame.columns
    ):
        raise RuntimeError(
            f"ThetaData calendar response for {day.isoformat()} has no type"
        )
    value = frame["type"][0]
    if value is None:
        raise RuntimeError(
            f"ThetaData calendar response for {day.isoformat()} has no type"
        )
    return str(value).strip().lower()


def _marker_row_count(body: bytes) -> int:
    try:
        payload = json.loads(body)
        count = payload["row_count"]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise RuntimeError("option day marker is missing row_count") from error
    if isinstance(count, bool) or not isinstance(count, int):
        raise RuntimeError("option day marker is missing row_count")
    return count


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
    with _thetadata_gate(settings) as gate:
        frame = fetch_symbol_frame(
            request.dataset,
            settings.thetadata_api_key,
            gate,
            heartbeat=activity.heartbeat,
        )
    body, row_count = build_symbol_csv(frame, date.fromisoformat(request.fetch_date))
    key = object_key(request.dataset, request.fetch_date)
    garage_store(settings).put_csv(key, body)
    activity.logger.info("uploaded %s rows=%s bytes=%s", key, row_count, len(body))
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


@activity.defn(name=RESOLVE_OPTION_SESSION)
def resolve_option_session_activity(observed_at: str) -> SessionResolution:
    settings = Settings.from_env()
    client_box: dict[str, object] = {}
    gate_box: dict[str, RedisGate | None] = {"gate": None}

    def schedule_type(day: date) -> str:
        activity.heartbeat()
        if gate_box["gate"] is None:
            gate_box["gate"] = redis_gate(settings)
        gate = gate_box["gate"]
        with gate.lease(OPTION_DATA_WORKFLOW, heartbeat=activity.heartbeat):
            client = client_box.get("client")
            if client is None:
                client = _theta_client(settings.thetadata_api_key)
                client_box["client"] = client
            frame = client.calendar_on_date(day)
        return _calendar_type(frame, day)

    try:
        resolution = resolve_option_session(
            observed_at,
            schedule_type=schedule_type,
        )
    finally:
        gate = gate_box["gate"]
        if gate is not None:
            gate.close()
    activity.logger.info(
        "option session skipped=%s trade_date=%s reason=%s",
        resolution.skipped,
        resolution.trade_date,
        resolution.skip_reason,
    )
    return resolution


@activity.defn(name=OPTION_DAY_LOADED)
def option_day_loaded_activity(trade_date: str) -> LoadedDay:
    store = garage_store(Settings.from_env())
    key = option_complete_key(trade_date)
    if not store.exists(key):
        return LoadedDay(False, 0)
    loaded = LoadedDay(True, _marker_row_count(store.get_bytes(key)))
    activity.logger.info(
        "option day %s is complete rows=%s", trade_date, loaded.row_count
    )
    return loaded


@activity.defn(name=CONFIRM_OPTION_OPEN_INTEREST)
def confirm_option_open_interest_activity(session: SessionResolution) -> None:
    activity.heartbeat()
    settings = Settings.from_env()
    trade_date = require_fetch_date(session.trade_date)
    oi_end = date.fromisoformat(require_fetch_date(session.oi_end_date))
    trade_day = date.fromisoformat(trade_date)
    symbol = open_interest_probe_symbol(latest_option_volumes(settings, trade_date))
    from thetadata.errors import NoDataFoundError

    frame = None
    with _thetadata_gate(settings) as gate:
        with gate.lease(OPTION_DATA_WORKFLOW, heartbeat=activity.heartbeat):
            client = _theta_client(settings.thetadata_api_key)
        with gate.lease(OPTION_DATA_WORKFLOW, heartbeat=activity.heartbeat):
            try:
                fetched = request_open_interest(client, symbol, oi_end)
            except NoDataFoundError:
                fetched = None
    if isinstance(fetched, pl.DataFrame):
        frame = fetched
    elif fetched is not None:
        raise RuntimeError(
            f"ThetaData open interest probe for {symbol} returned "
            f"{type(fetched).__name__}"
        )
    if open_interest_covers_session(frame, trade_day):
        activity.logger.info(
            "open interest print is ready trade_date=%s probe=%s",
            trade_date,
            symbol,
        )
        return
    activity.logger.info(
        "open interest print is not ready trade_date=%s probe=%s",
        trade_date,
        symbol,
    )
    raise ApplicationError(
        f"open interest print is not ready for {trade_date}",
        type=OPEN_INTEREST_NOT_READY,
    )


@activity.defn(name=PLAN_OPTION_BATCHES)
def plan_option_batches_activity(trade_date: str) -> OptionPlan:
    settings = Settings.from_env()
    day = require_fetch_date(trade_date)
    store = garage_store(settings)
    snapshot = _latest_snapshot(store)
    stocks = pl.read_csv(io.BytesIO(store.get_bytes(object_key("stocks", snapshot))))
    underlyings = pl.read_csv(
        io.BytesIO(store.get_bytes(object_key("option_underlyings", snapshot)))
    )
    symbols = intersect_symbols(stocks, underlyings)
    ordered = order_underlyings(symbols, latest_option_volumes(settings, day))
    batches = chunk_symbols(ordered, settings.thetadata_option_batch_size)
    activity.logger.info(
        "planned %s option underlyings from %s in %s batches",
        len(symbols),
        snapshot,
        len(batches),
    )
    return OptionPlan(
        symbol_snapshot_date=snapshot,
        underlying_count=len(symbols),
        batches=batches,
    )


@activity.defn(name=FETCH_OPTION_BATCH)
def fetch_option_batch_activity(batch: OptionBatch) -> BatchFetchResult:
    activity.heartbeat()
    settings = Settings.from_env()
    trade_date = require_fetch_date(batch.trade_date)
    oi_end_date = require_fetch_date(batch.oi_end_date)
    symbols = [require_option_symbol(symbol) for symbol in batch.symbols]
    store = garage_store(settings)
    missing = [
        symbol
        for symbol in symbols
        if not store.exists(option_symbol_key(trade_date, symbol))
    ]
    if not missing:
        return BatchFetchResult(0)
    trade_day = date.fromisoformat(trade_date)
    oi_end = date.fromisoformat(oi_end_date)
    fetched = 0
    errors: list[BaseException] = []
    with _thetadata_gate(settings) as gate:
        with gate.lease(OPTION_DATA_WORKFLOW, heartbeat=activity.heartbeat):
            client = _theta_client(settings.thetadata_api_key)
        fetched, errors = _fetch_missing(
            store,
            client,
            gate,
            trade_date,
            trade_day,
            oi_end,
            missing,
            settings.thetadata_max_concurrent,
        )
    if errors:
        raise errors[0]
    activity.logger.info(
        "fetched option batch %s for %s symbols=%s",
        batch.batch_index,
        trade_date,
        fetched,
    )
    return BatchFetchResult(fetched)


def _fetch_missing(
    store: GarageStore,
    client: object,
    gate: RedisGate,
    trade_date: str,
    trade_day: date,
    oi_end: date,
    missing: list[str],
    max_concurrent: int,
) -> tuple[int, list[BaseException]]:
    fetched = 0
    errors: list[BaseException] = []
    workers = max(1, max_concurrent)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                load_option_chain,
                client,
                gate,
                symbol,
                trade_day,
                oi_end,
                None,
            ): symbol
            for symbol in missing
        }
        pending = set(futures)
        while pending:
            done, pending = wait(pending, timeout=10, return_when=FIRST_COMPLETED)
            if not done:
                activity.heartbeat()
                continue
            for future in done:
                symbol = futures[future]
                try:
                    frame = future.result()
                    store.put_csv(
                        option_symbol_key(trade_date, symbol),
                        frame_to_csv(frame),
                    )
                except Exception as error:
                    errors.append(error)
                    activity.heartbeat()
                    continue
                fetched += 1
                activity.logger.info(
                    "checkpointed %s %s rows=%s",
                    trade_date,
                    symbol,
                    frame.height,
                )
                activity.heartbeat(symbol)
    return fetched, errors


@activity.defn(name=APPEND_OPTION_BATCH)
def append_option_batch_activity(batch: OptionBatch) -> BatchAppendResult:
    settings = Settings.from_env()
    trade_date = require_fetch_date(batch.trade_date)
    symbols = [require_option_symbol(symbol) for symbol in batch.symbols]
    store = garage_store(settings)
    bodies: list[bytes] = []
    for symbol in symbols:
        key = option_symbol_key(trade_date, symbol)
        if not store.exists(key):
            raise RuntimeError(f"missing option checkpoint {key}")
        bodies.append(store.get_bytes(key))
    part_key = option_part_key(trade_date, batch.batch_index)
    store.put_csv(part_key, concat_checkpoint_csvs(bodies))
    appended = append_option_csv(settings, trade_date, part_key, symbols)
    activity.logger.info(
        "appended option batch %s for %s rows=%s",
        batch.batch_index,
        trade_date,
        appended,
    )
    return BatchAppendResult(appended)


@activity.defn(name=COMPLETE_OPTION_DAY)
def complete_option_day_activity(trade_date: str) -> int:
    settings = Settings.from_env()
    day = require_fetch_date(trade_date)
    count = option_session_row_count(settings, day)
    if count < settings.thetadata_option_min_rows:
        raise RuntimeError(
            f"option_data has {count} rows for {day}; "
            f"expected at least {settings.thetadata_option_min_rows}"
        )
    garage_store(settings).put_bytes(
        option_complete_key(day),
        json.dumps({"row_count": count}).encode("utf-8"),
        "application/json",
    )
    activity.logger.info("completed option day %s rows=%s", day, count)
    return count


def _latest_snapshot(store: GarageStore) -> str:
    from thetadata_etl.datasets import latest_shared_snapshot

    return latest_shared_snapshot(
        store.list_keys(f"{KEY_PREFIX}/stocks/"),
        store.list_keys(f"{KEY_PREFIX}/option_underlyings/"),
    )
