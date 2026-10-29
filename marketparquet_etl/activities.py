from __future__ import annotations

import os

import requests
from temporalio import activity

from marketparquet_etl.catalog import (
    QuackCatalog,
    append_day,
    ensure_catalog,
    loaded_dates,
)
from marketparquet_etl.client import MarketParquetClient
from marketparquet_etl.config import Settings
from marketparquet_etl.discord import send_discord_message
from marketparquet_etl.limiter import redis_bucket
from marketparquet_etl.models import (
    APPEND_BATCH,
    APPEND_MARKETPARQUET_STOCK_DAYS,
    ENSURE_MARKETPARQUET_CATALOG,
    ETFS,
    FETCH_BATCH,
    FETCH_MARKETPARQUET_STOCK_DAYS,
    PLAN_MARKETPARQUET_STOCK_SYNC,
    SEND_MARKETPARQUET_DISCORD,
    STOCKS,
    AppendChunkResult,
    AssetChunk,
    AssetState,
    FetchChunkResult,
    StockPlan,
    object_key,
    require_asset,
    require_session_date,
)
from marketparquet_etl.normalize import parquet_to_csv
from marketparquet_etl.storage import (
    existing_csv_key,
    garage_store,
    stored_session_dates,
)


@activity.defn(name=ENSURE_MARKETPARQUET_CATALOG)
def ensure_marketparquet_catalog_activity() -> None:
    settings = Settings.from_env()
    garage_store(settings).ensure_bucket()
    client = QuackCatalog(settings.quack_uri, settings.quack_token)
    try:
        ensure_catalog(client, settings.iceberg_catalog_uri)
    finally:
        client.close()


@activity.defn(name=PLAN_MARKETPARQUET_STOCK_SYNC)
def plan_marketparquet_stock_sync_activity() -> StockPlan:
    settings = Settings.from_env()
    store = garage_store(settings)
    client = _api_client(settings)
    catalog = QuackCatalog(settings.quack_uri, settings.quack_token)
    try:
        stocks = _plan_asset(client, store, catalog, STOCKS)
        etfs = _plan_asset(client, store, catalog, ETFS)
    finally:
        catalog.close()
    activity.logger.info(
        "planned sync stocks published=%s csv=%s iceberg=%s "
        "etfs published=%s csv=%s iceberg=%s",
        len(stocks.dates),
        len(stocks.csv_dates),
        len(stocks.iceberg_dates),
        len(etfs.dates),
        len(etfs.csv_dates),
        len(etfs.iceberg_dates),
    )
    return StockPlan(stocks=stocks, etfs=etfs)


@activity.defn(name=FETCH_MARKETPARQUET_STOCK_DAYS)
def fetch_marketparquet_stock_days_activity(chunk: AssetChunk) -> FetchChunkResult:
    spec = require_asset(chunk.asset)
    dates = chunk.dates
    if not dates or len(dates) > FETCH_BATCH:
        raise ValueError(f"fetch chunk must contain 1 to {FETCH_BATCH} dates")
    days = [require_session_date(day) for day in dates]
    settings = Settings.from_env()
    store = garage_store(settings)
    client = _api_client(settings)
    pending = [day for day in days if existing_csv_key(store, spec.name, day) is None]
    urls: dict[str, str] = {}
    if pending:
        urls = client.manifest(pending[0], pending[-1], spec.api_type)
    written: list[str] = []
    counts: list[int] = []
    for day in pending:
        activity.heartbeat(f"{spec.name}:{day}")
        if existing_csv_key(store, spec.name, day) is not None:
            continue
        payload = _download_day(client, day, urls.get(day), spec.name)
        csv_body, rows = parquet_to_csv(
            payload, day, min_rows=spec.min_rows, asset_label=spec.label
        )
        store.put_csv(object_key(spec.name, day), csv_body)
        written.append(day)
        counts.append(rows)
    activity.logger.info(
        "fetched %s days requested=%s written=%s",
        spec.name,
        len(days),
        len(written),
    )
    return FetchChunkResult(dates=written, row_counts=counts)


@activity.defn(name=APPEND_MARKETPARQUET_STOCK_DAYS)
def append_marketparquet_stock_days_activity(chunk: AssetChunk) -> AppendChunkResult:
    spec = require_asset(chunk.asset)
    dates = chunk.dates
    if not dates or len(dates) > APPEND_BATCH:
        raise ValueError(f"append chunk must contain 1 to {APPEND_BATCH} dates")
    days = [require_session_date(day) for day in dates]
    settings = Settings.from_env()
    store = garage_store(settings)
    catalog = QuackCatalog(settings.quack_uri, settings.quack_token)
    appended_days = 0
    appended_rows = 0
    already_stored = 0
    try:
        ensure_catalog(catalog, settings.iceberg_catalog_uri)
        for day in days:
            activity.heartbeat(f"{spec.name}:{day}")
            key = existing_csv_key(store, spec.name, day)
            if key is None:
                raise RuntimeError(f"missing {spec.name} CSV for {day}")
            appended, rows = append_day(catalog, day, spec.name, key)
            if appended:
                appended_days += 1
                appended_rows += rows
            else:
                already_stored += 1
    finally:
        catalog.close()
    activity.logger.info(
        "appended %s days requested=%s appended=%s rows=%s",
        spec.name,
        len(days),
        appended_days,
        appended_rows,
    )
    return AppendChunkResult(
        appended_days=appended_days,
        appended_rows=appended_rows,
        already_stored=already_stored,
    )


@activity.defn(name=SEND_MARKETPARQUET_DISCORD)
def send_marketparquet_discord_message_activity(content: str) -> None:
    status = send_discord_message(content)
    activity.logger.info(
        "sent MarketParquet Discord message status=%s title=%s",
        status,
        content.split("\n", 1)[0],
    )


def _api_client(settings: Settings) -> MarketParquetClient:
    timeout = float(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))
    return MarketParquetClient(
        settings.marketparquet_api_key,
        redis_bucket(settings.redis_url, settings.requests_per_minute),
        heartbeat=activity.heartbeat,
        timeout=timeout,
    )


def _plan_asset(client, store, catalog, asset: str) -> AssetState:
    spec = require_asset(asset)
    dates = client.list_dates(spec.api_type)
    activity.heartbeat(f"{spec.name}:dates")
    csv_dates = stored_session_dates(store, spec.name)
    activity.heartbeat(f"{spec.name}:csv")
    iceberg_dates = loaded_dates(catalog, spec.name)
    return AssetState(
        asset=spec.name,
        dates=dates,
        csv_dates=csv_dates,
        iceberg_dates=iceberg_dates,
    )


def _download_day(
    client: MarketParquetClient,
    day: str,
    url: str | None,
    asset: str = STOCKS,
) -> bytes:
    spec = require_asset(asset)
    if url:
        try:
            return client.get_bytes(url)
        except requests.RequestException:
            activity.logger.info(
                "presigned download failed for %s %s; requesting a new URL",
                spec.name,
                day,
            )
    return client.download_day(day, spec.api_type)
