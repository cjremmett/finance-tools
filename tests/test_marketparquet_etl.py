import asyncio
import io
from datetime import date, datetime, timedelta

import polars as pl
import pytest
import requests
from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from marketparquet_etl.activities import _download_day
from marketparquet_etl.catalog import (
    SECRET_SQL,
    attach_sql,
    count_sql,
    create_table_sql,
    insert_sql,
    loaded_dates_sql,
)
from marketparquet_etl.client import MarketParquetClient, retry_after_seconds
from marketparquet_etl.config import Settings
from marketparquet_etl.discord import (
    DISCORD_CONTENT_LIMIT,
    build_done_message,
    build_failure_message,
    build_idle_message,
    build_start_message,
    failure_reason,
)
from marketparquet_etl.limiter import MemoryBucket
from marketparquet_etl.models import (
    ETF_MIN_ROWS,
    ETFS,
    MANIFEST_LIMIT,
    MIN_ROWS,
    STOCKS,
    AppendChunkResult,
    AssetState,
    FetchChunkResult,
    StockPlan,
    WorkflowResult,
    append_batches,
    fetch_batches,
    legacy_stock_key,
    object_key,
    require_csv_key,
    require_session_date,
)
from marketparquet_etl.normalize import parquet_to_csv
from marketparquet_etl.storage import GarageStore, stored_session_dates
from marketparquet_etl.workflows import MarketParquetStockDataWorkflow


class _Limiter:
    def __init__(self) -> None:
        self.acquires = 0

    def acquire(self, sleep=None) -> None:
        self.acquires += 1


class _Response:
    def __init__(self, payload, status=200, headers=None) -> None:
        self.status_code = status
        self.headers = headers or {}
        self._payload = payload
        self.content = payload if isinstance(payload, bytes) else b""

    def json(self):
        return self._payload


class _Session:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls = []

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append((url, params, headers))
        if not self.responses:
            raise AssertionError(f"unexpected GET {url}")
        return self.responses.pop(0)


def _dates(count: int, start: date = date(2026, 1, 1)) -> list[str]:
    return [(start + timedelta(days=offset)).isoformat() for offset in range(count)]


def _parquet(
    count: int,
    *,
    timestamp: bool = False,
    volume: float = 100.0,
    duplicate: bool = False,
    null_close: bool = False,
    bad_date: bool = False,
    asset_type: str = "Stock",
) -> bytes:
    day = date(2026, 3, 26) if timestamp else date(2026, 10, 27)
    symbols = [f"S{index:04d}" for index in range(count)]
    if duplicate and count > 1:
        symbols[1] = symbols[0]
    closes: list[float | None] = [1.25] * count
    if null_close:
        closes[0] = None
    volumes = [volume] + [100.0] * (count - 1)
    frame = {
        "symbol": symbols,
        "asset_type": [asset_type] * count,
        "open": [1.0] * count,
        "high": [2.0] * count,
        "low": [0.5] * count,
        "close": closes,
        "volume": volumes,
    }
    if timestamp:
        stamps = [datetime(2026, 3, 26)] * count
        if bad_date and count > 1:
            stamps[1] = datetime(2026, 3, 27)
        frame["timestamp"] = stamps
    else:
        dates = [day] * count
        if bad_date and count > 1:
            dates[1] = date(2026, 10, 26)
        frame["date"] = dates
    buffer = io.BytesIO()
    pl.DataFrame(frame).write_parquet(buffer)
    return buffer.getvalue()


def _required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MARKETPARQUET_API_KEY", "bt_test")
    monkeypatch.setenv("GARAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("GARAGE_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("QUACK_TOKEN", "token")
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/0")
    monkeypatch.setenv(
        "ETL_PIPELINE_DISCORD_WEBHOOK", "https://discord.test/webhook"
    )
    monkeypatch.delenv("MARKETPARQUET_REQUESTS_PER_MINUTE", raising=False)


def test_timestamp_and_date_files_share_one_csv_schema() -> None:
    recent, recent_rows = parquet_to_csv(
        _parquet(MIN_ROWS), "2026-10-27"
    )
    older, older_rows = parquet_to_csv(
        _parquet(MIN_ROWS, timestamp=True, volume=428179.592521),
        "2026-03-26",
    )
    assert recent_rows == MIN_ROWS
    assert older_rows == MIN_ROWS
    assert recent.decode().splitlines()[0] == (
        "date,symbol,asset_type,open,high,low,close,volume"
    )
    assert older.decode().splitlines()[0] == recent.decode().splitlines()[0]
    restored = pl.read_csv(
        io.BytesIO(older), schema_overrides={"date": pl.Date}
    )
    assert "428179.592521" in older.decode()
    assert restored["date"].to_list()[0] == date(2026, 3, 26)
    assert restored["volume"][0] == pytest.approx(428179.592521)
    assert "timestamp" not in restored.columns


def test_normalize_rejects_bad_files() -> None:
    with pytest.raises(ValueError, match="null values"):
        parquet_to_csv(_parquet(3, null_close=True), "2026-10-27")
    with pytest.raises(ValueError, match="duplicate symbols"):
        parquet_to_csv(_parquet(3, duplicate=True), "2026-10-27")
    with pytest.raises(ValueError, match="another trading date"):
        parquet_to_csv(_parquet(3, bad_date=True), "2026-10-27")
    with pytest.raises(ValueError, match="another trading date"):
        parquet_to_csv(
            _parquet(3, timestamp=True, bad_date=True), "2026-03-26"
        )
    with pytest.raises(ValueError, match="expected at least"):
        parquet_to_csv(_parquet(MIN_ROWS - 1), "2026-10-27")
    etf, etf_rows = parquet_to_csv(
        _parquet(ETF_MIN_ROWS, asset_type="ETF"),
        "2026-10-27",
        min_rows=ETF_MIN_ROWS,
        asset_label="ETF",
    )
    assert etf_rows == ETF_MIN_ROWS
    assert etf.decode().splitlines()[1].split(",")[2] == "ETF"
    with pytest.raises(ValueError, match="expected at least 3000"):
        parquet_to_csv(
            _parquet(ETF_MIN_ROWS - 1, asset_type="ETF"),
            "2026-10-27",
            min_rows=ETF_MIN_ROWS,
            asset_label="ETF",
        )
    with pytest.raises(ValueError, match="expected ETF"):
        parquet_to_csv(
            _parquet(ETF_MIN_ROWS),
            "2026-10-27",
            min_rows=ETF_MIN_ROWS,
            asset_label="ETF",
        )
    with pytest.raises(ValueError, match="neither date nor timestamp"):
        frame = pl.DataFrame(
            {
                "symbol": ["A"],
                "asset_type": ["Stock"],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1.0],
            }
        )
        buffer = io.BytesIO()
        frame.write_parquet(buffer)
        parquet_to_csv(buffer.getvalue(), "2026-10-27")


def test_date_pagination_stops_on_a_short_page() -> None:
    first = [{"date": day} for day in _dates(100)]
    second = [{"date": day} for day in _dates(2, date(2026, 5, 1))]
    session = _Session(
        [
            _Response({"dates": first, "per_page": 100, "page": 1}),
            _Response({"dates": second, "per_page": 100, "page": 2}),
        ]
    )
    client = MarketParquetClient("bt_test", _Limiter(), session=session)
    dates = client.list_dates()
    assert dates == sorted(day["date"] for day in first + second)
    assert len(session.calls) == 2
    assert session.calls[0][0].endswith("/dates/stock_daily")
    assert session.calls[0][1] == {"page": 1}
    assert session.calls[1][1] == {"page": 2}
    assert session.calls[0][2]["Authorization"] == "Bearer bt_test"
    etf_session = _Session([_Response({"dates": [], "per_page": 100})])
    assert (
        MarketParquetClient("bt_test", _Limiter(), session=etf_session).list_dates(
            "etf_daily"
        )
        == []
    )
    assert etf_session.calls[0][0].endswith("/dates/etf_daily")
    with pytest.raises(ValueError, match="unsupported"):
        client.list_dates("futures_daily")


def test_rate_limit_response_waits_and_retries() -> None:
    slept: list[float] = []
    limiter = _Limiter()
    session = _Session(
        [
            _Response({}, status=429, headers={"Retry-After": "1"}),
            _Response({"dates": [], "per_page": 100}),
        ]
    )
    client = MarketParquetClient(
        "bt_test", limiter, session=session, sleeper=slept.append
    )
    assert client.list_dates() == []
    assert limiter.acquires == 2
    assert slept == [1.0]


def test_retry_after_defaults_and_caps() -> None:
    assert retry_after_seconds({}) == 60
    assert retry_after_seconds({"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}) == 60
    assert retry_after_seconds({"Retry-After": "-5"}) == 60
    assert retry_after_seconds({"Retry-After": "500"}) == 120
    assert retry_after_seconds({"Retry-After": "12.5"}) == 12.5


def test_presigned_download_is_not_rate_limited_and_falls_back() -> None:
    limiter = _Limiter()
    session = _Session(
        [
            _Response(
                {"download_url": "https://files.example/day.parquet"}
            ),
            _Response(b"parquet-bytes"),
        ]
    )
    client = MarketParquetClient("bt_test", limiter, session=session)
    assert client.download_day("2026-10-27") == b"parquet-bytes"
    assert limiter.acquires == 1
    assert session.calls[0][0].endswith("/download/stock_daily/2026-10-27")
    assert session.calls[1][0] == "https://files.example/day.parquet"
    etf_session = _Session(
        [
            _Response({"download_url": "https://files.example/etf.parquet"}),
            _Response(b"etf-bytes"),
        ]
    )
    etf_client = MarketParquetClient("bt_test", _Limiter(), session=etf_session)
    assert etf_client.download_day("2026-10-27", "etf_daily") == b"etf-bytes"
    assert etf_session.calls[0][0].endswith("/download/etf_daily/2026-10-27")

    class _Expired:
        def get_bytes(self, url: str) -> bytes:
            raise requests.ConnectionError(url)

        def download_day(self, day: str, asset_type: str = "stock_daily") -> bytes:
            assert asset_type == "etf_daily"
            return b"fallback"

    assert _download_day(
        _Expired(), "2026-10-27", "https://files.example/x", ETFS
    ) == b"fallback"


def test_bucket_cannot_grant_more_than_its_rate_in_one_minute() -> None:
    clock = {"now": 0.0}
    bucket = MemoryBucket(rpm=50, now=lambda: clock["now"])
    granted = 0
    for _ in range(60):
        if bucket.try_take() == 0:
            granted += 1
    assert granted == 50
    assert bucket.try_take() > 0
    clock["now"] = 60.0
    assert bucket.try_take() == 0


def test_missing_days_are_grouped_and_capped() -> None:
    published = _dates(90)
    present = {published[10], published[50]}
    chunks = fetch_batches(published, sorted(present))
    assert chunks == [
        published[:10],
        published[11:50],
        published[51:],
    ]
    long_run = _dates(MANIFEST_LIMIT + 1, date(2024, 1, 1))
    long_chunks = fetch_batches(long_run, [])
    assert [len(chunk) for chunk in long_chunks] == [40] * 10 + [1]
    assert sum(len(chunk) for chunk in long_chunks) == MANIFEST_LIMIT + 1
    assert append_batches(published, [published[0], published[2]])[0][:2] == [
        published[1],
        published[3],
    ]


def test_object_key_and_sql_stay_inside_the_session() -> None:
    assert object_key(STOCKS, "2026-10-27") == (
        "securities/stock_data/stocks-2026-10-27.csv"
    )
    assert object_key(ETFS, "2026-10-27") == (
        "securities/stock_data/etfs-2026-10-27.csv"
    )
    assert legacy_stock_key("2026-10-27") == (
        "securities/stock_data/2026-10-27.csv"
    )
    with pytest.raises(ValueError):
        require_session_date("2026-10-27; drop table stock_data")
    with pytest.raises(ValueError):
        object_key("futures", "2026-10-27")
    with pytest.raises(ValueError):
        insert_sql("2026-1-2")
    sql = insert_sql("2026-10-27")
    assert "s3://securities-data/securities/stock_data/stocks-2026-10-27.csv" in sql
    assert "WHERE date = DATE '2026-10-27' AND asset_type = 'Stock'" in sql
    assert "'volume': 'DOUBLE'" in sql
    legacy = insert_sql("2026-10-27", key=legacy_stock_key("2026-10-27"))
    assert "securities/stock_data/2026-10-27.csv" in legacy
    etf_sql = insert_sql("2026-10-27", ETFS)
    assert "etfs-2026-10-27.csv" in etf_sql
    assert "asset_type = 'ETF'" in etf_sql
    with pytest.raises(ValueError):
        insert_sql("2026-10-27", ETFS, key=legacy_stock_key("2026-10-27"))
    with pytest.raises(ValueError):
        require_csv_key(STOCKS, "securities/stock_data/etfs-2026-10-27.csv")
    create = create_table_sql()
    assert "securities_data.securities.stock_data" in create
    assert "volume DOUBLE" in create
    assert "DATE '2026-03-26'" in count_sql("2026-03-26")
    assert "asset_type = 'Stock'" in count_sql("2026-03-26")
    assert "asset_type = 'ETF'" in count_sql("2026-03-26", ETFS)
    assert "HAVING count(*) >= 5000" in loaded_dates_sql(STOCKS)
    assert "HAVING count(*) >= 3000" in loaded_dates_sql(ETFS)
    assert "getenv('GARAGE_ACCESS_KEY_ID')" in SECRET_SQL
    assert "getenv('GARAGE_SECRET_ACCESS_KEY')" in SECRET_SQL
    assert "s3://securities-data" in SECRET_SQL
    with pytest.raises(ValueError):
        attach_sql("http://catalog.example/' ;")


def test_bucket_is_created_only_when_missing() -> None:
    from botocore.exceptions import ClientError

    class _S3:
        def __init__(self, missing: bool) -> None:
            self.missing = missing
            self.created = False

        def head_bucket(self, Bucket: str) -> None:
            if self.missing:
                raise ClientError(
                    {
                        "Error": {"Code": "404", "Message": "Not Found"},
                        "ResponseMetadata": {"HTTPStatusCode": 404},
                    },
                    "HeadBucket",
                )

        def create_bucket(self, Bucket: str) -> None:
            self.created = True

    missing = _S3(True)
    GarageStore(missing, "securities-data").ensure_bucket()
    assert missing.created
    present = _S3(False)
    GarageStore(present, "securities-data").ensure_bucket()
    assert not present.created


def test_stored_csv_dates_ignore_other_keys() -> None:
    class _Store:
        def list_keys(self, prefix: str) -> list[str]:
            assert prefix == "securities/stock_data/"
            return [
                "securities/stock_data/2026-10-27.csv",
                "securities/stock_data/stocks-2026-03-26.csv",
                "securities/stock_data/etfs-2026-10-27.csv",
                "securities/stock_data/notes.txt",
                "securities/stock_data/etfs-2026-03-26.csv",
            ]

    store = _Store()
    assert stored_session_dates(store) == ["2026-03-26", "2026-10-27"]
    assert stored_session_dates(store, ETFS) == ["2026-03-26", "2026-10-27"]


def _result(**overrides) -> WorkflowResult:
    fields = dict(
        workflow_run_id="run-1",
        window_start="2026-03-26",
        window_end="2026-10-27",
        session_count=3,
        downloaded=2,
        reused=1,
        appended_days=2,
        appended_rows=12_000,
        already_stored=1,
        idle=False,
        stock_sessions=2,
        etf_sessions=1,
        stock_downloaded=1,
        etf_downloaded=1,
        stock_appended_days=2,
        etf_appended_days=0,
        stock_appended_rows=12_000,
        etf_appended_rows=0,
    )
    fields.update(overrides)
    return WorkflowResult(**fields)


def test_discord_messages_name_the_run_and_stay_within_the_limit() -> None:
    plan = StockPlan(
        stocks=AssetState(
            STOCKS,
            ["2026-03-26", "2026-10-27"],
            ["2026-03-26"],
            [],
        ),
        etfs=AssetState(ETFS, ["2026-10-27"], [], ["2026-10-27"]),
    )
    start = build_start_message(plan, "run-1")
    assert "To download: Stocks 1, ETFs 1" in start
    assert "To append: Stocks 2, ETFs 0" in start
    assert "run-1" in start
    done = build_done_message(_result())
    idle = build_idle_message(
        _result(
            workflow_run_id="run-2",
            window_start="2025-10-28",
            window_end="2026-10-27",
            session_count=501,
            downloaded=0,
            reused=501,
            appended_days=0,
            appended_rows=0,
            already_stored=501,
            idle=True,
            stock_sessions=251,
            etf_sessions=250,
            stock_downloaded=0,
            etf_downloaded=0,
            stock_appended_days=0,
            etf_appended_days=0,
            stock_appended_rows=0,
            etf_appended_rows=0,
        )
    )
    failure = build_failure_message("run-3", "x" * 5000)
    assert "Downloaded: Stocks 1, ETFs 1" in done
    assert "Already stored: Stocks 0, ETFs 1" in done
    assert "nothing to do" in idle
    assert all(
        len(message) <= DISCORD_CONTENT_LIMIT for message in (start, done, idle, failure)
    )
    error = ActivityError(
        "Activity task failed",
        scheduled_event_id=1,
        started_event_id=2,
        identity="worker",
        activity_type="fetch_marketparquet_stock_days",
        activity_id="1",
        retry_state=None,
    )
    error.__cause__ = ApplicationError("manifest failed", type="RuntimeError")
    assert failure_reason(error) == (
        "fetch_marketparquet_stock_days: RuntimeError: manifest failed"
    )


def test_settings_require_the_key_and_cap_the_rate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _required(monkeypatch)
    monkeypatch.delenv("MARKETPARQUET_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="MARKETPARQUET_API_KEY"):
        Settings.from_env()

    _required(monkeypatch)
    monkeypatch.setenv("REDIS_URL", "http://redis:6379/0")
    with pytest.raises(RuntimeError, match="REDIS_URL"):
        Settings.from_env()

    _required(monkeypatch)
    settings = Settings.from_env()
    assert settings.requests_per_minute == 50
    assert settings.temporal_task_queue == "marketparquet-etl"

    _required(monkeypatch)
    monkeypatch.setenv("MARKETPARQUET_REQUESTS_PER_MINUTE", "61")
    with pytest.raises(RuntimeError, match="MARKETPARQUET_REQUESTS_PER_MINUTE"):
        Settings.from_env()
    monkeypatch.setenv("MARKETPARQUET_REQUESTS_PER_MINUTE", "0")
    with pytest.raises(RuntimeError, match="MARKETPARQUET_REQUESTS_PER_MINUTE"):
        Settings.from_env()
    monkeypatch.setenv("MARKETPARQUET_REQUESTS_PER_MINUTE", "60")
    assert Settings.from_env().requests_per_minute == 60


def test_workflow_passes_temporal_sandbox_validation() -> None:
    async def validate() -> None:
        SandboxedWorkflowRunner().prepare_workflow(
            workflow._Definition.must_from_class(MarketParquetStockDataWorkflow)
        )

    asyncio.run(validate())


def test_chunk_results_record_what_was_written() -> None:
    fetched = FetchChunkResult(dates=["2026-10-27"], row_counts=[6499])
    appended = AppendChunkResult(appended_days=1, appended_rows=6499, already_stored=0)
    assert fetched.row_counts == [6499]
    assert appended.appended_rows == 6499
