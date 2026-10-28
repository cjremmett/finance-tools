import asyncio
import io
from contextlib import nullcontext
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
from thetadata.errors import NoDataFoundError

from thetadata_etl.calendar import SKIP_CLOSED_MORNING, resolve_option_session
from thetadata_etl.catalog import (
    SCHEMA_SQL,
    SECRET_SQL,
    append_csv,
    append_option_part,
    attach_sql,
    count_sql,
    create_option_data_sql,
    create_option_volume_sql,
    create_table_sql,
    insert_sql,
    latest_volume_sql,
    option_insert_sql,
    symbols_present_sql,
    volume_insert_sql,
)
from thetadata_etl.config import DEFAULT_JOB_PRIORITIES, Settings
from thetadata_etl.datasets import (
    OPTION_DATA_TABLE,
    OPTION_VOLUME_TABLE,
    eastern_fetch_date,
    latest_shared_snapshot,
    object_key,
    option_symbol_key,
    require_catalog_uri,
    require_fetch_date,
    require_option_part_key,
    require_option_symbol,
)
from thetadata_etl.models import SessionResolution
from thetadata_etl.options import (
    OPTION_COLUMNS,
    build_option_frame,
    chunk_symbols,
    concat_checkpoint_csvs,
    empty_option_frame,
    frame_to_csv,
    intersect_symbols,
    load_option_chain,
    open_interest_covers_session,
    open_interest_probe_symbol,
    order_underlyings,
)
from thetadata_etl.symbols import build_symbol_csv
from thetadata_etl.workflows import (
    ThetaDataOptionDataWorkflow,
    ThetaDataSecuritiesWorkflow,
)


def test_eastern_fetch_date_uses_new_york_calendar_day() -> None:
    assert eastern_fetch_date("2026-10-27T03:30:00+00:00") == "2026-10-26"
    assert eastern_fetch_date("2026-10-27T12:00:00+00:00") == "2026-10-27"
    naive = datetime(2026, 10, 27, 3, 30, tzinfo=timezone.utc).replace(tzinfo=None)
    assert eastern_fetch_date(naive.isoformat()) == "2026-10-26"


def test_object_key_uses_dataset_and_date() -> None:
    assert object_key("stocks", "2026-10-27") == "securities/stocks/2026-10-27.csv"
    assert (
        object_key("option_underlyings", "2026-10-27")
        == "securities/option_underlyings/2026-10-27.csv"
    )


def test_fetch_date_and_catalog_uri_reject_injection() -> None:
    with pytest.raises(ValueError):
        require_fetch_date("2026-10-27'; DROP TABLE stocks")
    with pytest.raises(ValueError):
        object_key("stocks", "2026-13-40")
    with pytest.raises(ValueError):
        require_catalog_uri("http://192.168.0.20:8181/catalog'; ATTACH")
    with pytest.raises(ValueError):
        object_key("positions", "2026-10-27")


def test_symbol_csv_adds_date_and_rejects_short_frames() -> None:
    frame = pl.DataFrame({"symbol": ["AAPL", " ", None, "MSFT"]})
    body, row_count = build_symbol_csv(
        frame, date(2026, 10, 27), minimum_rows=2
    )
    assert row_count == 2
    assert body.decode() == "symbol,date\nAAPL,2026-10-27\nMSFT,2026-10-27\n"
    with pytest.raises(RuntimeError, match="expected at least 3"):
        build_symbol_csv(frame, date(2026, 10, 27), minimum_rows=3)


def test_iceberg_sql_targets_securities_tables() -> None:
    assert "s3://securities-data" in SECRET_SQL
    assert "getenv('GARAGE_SECRET_ACCESS_KEY')" in SECRET_SQL
    assert "TYPE iceberg" in attach_sql("http://192.168.0.20:8181/catalog")
    assert SCHEMA_SQL == "CREATE SCHEMA IF NOT EXISTS securities_data.securities"
    assert (
        create_table_sql("stocks")
        == "CREATE TABLE IF NOT EXISTS securities_data.securities.stocks (symbol VARCHAR, date DATE)"
    )
    assert "DATE '2026-10-27'" in count_sql("option_underlyings", "2026-10-27")
    insert = insert_sql("stocks", "2026-10-27")
    assert "s3://securities-data/securities/stocks/2026-10-27.csv" in insert
    assert "{'symbol': 'VARCHAR', 'date': 'DATE'}" in insert


class FakeQuack:
    def __init__(self, counts: list[int]) -> None:
        self.counts = counts
        self.statements: list[str] = []

    def execute(self, sql: str) -> list[tuple]:
        self.statements.append(sql)
        return []

    def scalar(self, sql: str) -> int:
        self.statements.append(sql)
        return self.counts.pop(0)


def test_append_skips_a_date_already_loaded() -> None:
    client = FakeQuack([26_927])
    appended, row_count = append_csv(
        client, "http://192.168.0.20:8181/catalog", "stocks", "2026-10-27"
    )
    assert appended is False
    assert row_count == 26_927
    assert not any(statement.startswith("INSERT") for statement in client.statements)


def test_append_inserts_when_the_date_is_absent() -> None:
    client = FakeQuack([0, 15_831])
    appended, row_count = append_csv(
        client,
        "http://192.168.0.20:8181/catalog",
        "option_underlyings",
        "2026-10-27",
    )
    assert appended is True
    assert row_count == 15_831
    assert any(statement.startswith("INSERT INTO") for statement in client.statements)


def test_append_rejects_a_short_existing_snapshot() -> None:
    client = FakeQuack([12])
    with pytest.raises(RuntimeError, match="expected at least"):
        append_csv(client, "http://192.168.0.20:8181/catalog", "stocks", "2026-10-27")


def test_settings_require_quack_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUACK_TOKEN", raising=False)
    monkeypatch.setenv("THETADATA_API_KEY", "td")
    monkeypatch.setenv("GARAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("GARAGE_SECRET_ACCESS_KEY", "secret")
    with pytest.raises(RuntimeError, match="QUACK_TOKEN"):
        Settings.from_env()


def test_workflow_passes_temporal_sandbox_validation() -> None:
    async def validate() -> None:
        runner = SandboxedWorkflowRunner()
        for workflow_class in (
            ThetaDataSecuritiesWorkflow,
            ThetaDataOptionDataWorkflow,
        ):
            runner.prepare_workflow(workflow._Definition.must_from_class(workflow_class))

    asyncio.run(validate())


def test_calendar_walk_selects_the_previous_trading_session() -> None:
    resolution = resolve_option_session(
        "2026-10-27T10:50:00+00:00",
        schedule_type=lambda day: "open",
    )
    assert resolution == SessionResolution("2026-10-26", "2026-10-27", False, "")

    def weekday(day: date) -> str:
        if day.weekday() >= 5:
            return "weekend"
        return "open"

    monday = resolve_option_session(
        "2026-10-26T11:00:00+00:00",
        schedule_type=weekday,
    )
    assert monday.trade_date == "2026-10-23"
    assert monday.oi_end_date == "2026-10-26"
    assert monday.skipped is False

    early_close = resolve_option_session(
        "2026-10-27T10:50:00+00:00",
        schedule_type=lambda day: "early_close" if day.isoformat() == "2026-10-26" else "open",
    )
    assert early_close.trade_date == "2026-10-26"


def test_calendar_skips_closed_mornings_and_early_runs() -> None:
    closed_calls: list[date] = []

    def closed(day: date) -> str:
        closed_calls.append(day)
        return "full_close"

    holiday = resolve_option_session(
        "2026-10-27T10:50:00+00:00",
        schedule_type=closed,
    )
    assert holiday.skipped is True
    assert holiday.skip_reason == SKIP_CLOSED_MORNING
    assert closed_calls == [date(2026, 10, 27)]

    weekend = resolve_option_session(
        "2026-10-25T14:00:00+00:00",
        schedule_type=lambda day: "weekend",
    )
    assert weekend.skip_reason == SKIP_CLOSED_MORNING

    before_print = resolve_option_session(
        "2026-10-27T10:30:00+00:00",
        schedule_type=lambda day: "open",
    )
    assert before_print == SessionResolution("2026-10-26", "2026-10-27", False, "")
    naive = resolve_option_session(
        "2026-10-27T10:30:00",
        schedule_type=lambda day: "open",
    )
    assert naive.trade_date == "2026-10-26"
    assert naive.skipped is False


def test_calendar_rejects_a_long_closure_and_unknown_types() -> None:
    def closed(day: date) -> str:
        if day == date(2026, 10, 27):
            return "open"
        return "full_close"

    with pytest.raises(RuntimeError, match="no trading session"):
        resolve_option_session(
            "2026-10-27T10:50:00+00:00",
            schedule_type=closed,
        )
    with pytest.raises(RuntimeError, match="unexpected calendar type"):
        resolve_option_session(
            "2026-10-27T10:50:00+00:00",
            schedule_type=lambda day: "holiday",
        )


def test_option_universe_uses_the_latest_shared_snapshot() -> None:
    stocks = pl.DataFrame({"symbol": ["MSFT", " AAPL ", "", None, "SPX"]})
    options = pl.DataFrame({"symbol": ["SPX", "IBM", "AAPL"]})
    assert intersect_symbols(stocks, options) == ["AAPL", "SPX"]
    with pytest.raises(RuntimeError, match="no symbols in common"):
        intersect_symbols(
            pl.DataFrame({"symbol": ["AAPL"]}),
            pl.DataFrame({"symbol": ["IBM"]}),
        )
    assert (
        latest_shared_snapshot(
            [
                "securities/stocks/2026-10-26.csv",
                "securities/stocks/2026-10-27.csv",
                "securities/stocks/2026-10-28/nested.csv",
                "securities/stocks/not-a-date.csv",
            ],
            [
                "securities/option_underlyings/2026-10-25.csv",
                "securities/option_underlyings/2026-10-26.csv",
            ],
        )
        == "2026-10-26"
    )
    with pytest.raises(RuntimeError, match="no dated symbol snapshot"):
        latest_shared_snapshot(
            ["securities/stocks/2026-10-27.csv"],
            ["securities/option_underlyings/2026-10-26.csv"],
        )
    assert (
        option_symbol_key("2026-10-26", "BRK.B")
        == "securities/option_data/2026-10-26/symbols/BRK.B.csv"
    )
    with pytest.raises(ValueError):
        require_option_symbol("AAPL;DROP")
    with pytest.raises(ValueError):
        require_option_part_key("securities/option_data/2026-10-26/part-1.csv")


def test_option_fetch_order_uses_prior_volume() -> None:
    ordered = order_underlyings(
        ["MSFT", "AAPL", "TSLA", "QQQ"],
        {"TSLA": 5, "AAPL": 5, "MSFT": 1},
    )
    assert ordered == ["AAPL", "TSLA", "MSFT", "QQQ"]
    assert chunk_symbols(ordered, 2) == [["AAPL", "TSLA"], ["MSFT", "QQQ"]]


def _sample_eod() -> pl.DataFrame:
    created = datetime(2026, 10, 23, 16, 15, tzinfo=ZoneInfo("America/New_York"))
    return pl.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "expiration": [date(2026, 11, 20)] * 3,
            "strike": [150.0, 155.0, 160.0],
            "right": ["CALL", "PUT", "call"],
            "open": [1.0, 2.0, 3.0],
            "high": [1.2, 2.2, 3.2],
            "low": [0.8, 1.8, 2.8],
            "close": [1.1, 2.1, 3.1],
            "volume": [10, 3, 1],
            "created": [created, created, created],
            "last_trade": [
                datetime(2026, 10, 23, 16, 0, tzinfo=ZoneInfo("America/New_York")),
                datetime(2026, 10, 23, 15, 59, tzinfo=ZoneInfo("America/New_York")),
                datetime(2026, 10, 23, 16, 0, tzinfo=ZoneInfo("America/New_York")),
            ],
        }
    )


def test_open_interest_keeps_the_next_morning_print_and_fills_zeros() -> None:
    trade = date(2026, 10, 23)
    eastern = ZoneInfo("America/New_York")
    interest = pl.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL"],
            "expiration": [date(2026, 11, 20)] * 4,
            "strike": [150.0, 150.0, 150.0, 155.0],
            "right": ["call", "call", "call", "put"],
            "timestamp": [
                datetime(2026, 10, 23, 6, 30, tzinfo=eastern),
                datetime(2026, 10, 26, 6, 30, tzinfo=eastern),
                datetime(2026, 10, 26, 6, 31, tzinfo=eastern),
                datetime(2026, 10, 23, 6, 30, tzinfo=eastern),
            ],
            "open_interest": [5, 40, 41, 7],
        }
    )
    frame = build_option_frame(_sample_eod(), interest, trade, "AAPL").sort("strike")
    assert frame["open_interest"].to_list() == [41, 0, 0]
    assert frame["right"].to_list() == ["call", "put", "call"]
    assert frame["date"].to_list() == [trade, trade, trade]
    assert frame.columns == list(OPTION_COLUMNS)

    zeros = build_option_frame(_sample_eod(), None, trade, "AAPL")
    assert zeros["open_interest"].to_list() == [0, 0, 0]
    omitted = pl.DataFrame(
        {"timestamp": [datetime(2026, 10, 26, 6, 30, tzinfo=eastern)]}
    )
    assert (
        build_option_frame(_sample_eod(), omitted, trade, "AAPL")["open_interest"].to_list()
        == [0, 0, 0]
    )
    assert build_option_frame(None, interest, trade, "AAPL").height == 0
    assert build_option_frame(pl.DataFrame(), interest, trade, "AAPL").height == 0
    with pytest.raises(RuntimeError, match="missing"):
        build_option_frame(_sample_eod().drop("open"), interest, trade, "AAPL")
    with pytest.raises(RuntimeError, match="contains symbols"):
        build_option_frame(_sample_eod(), interest, trade, "MSFT")


def test_open_interest_probe_accepts_only_a_print_after_the_trade_date() -> None:
    trade = date(2026, 10, 26)
    eastern = ZoneInfo("America/New_York")
    stale = pl.DataFrame(
        {
            "symbol": ["AAPL"],
            "expiration": [date(2026, 11, 20)],
            "strike": [150.0],
            "right": ["CALL"],
            "timestamp": [datetime(2026, 10, 26, 6, 30, tzinfo=eastern)],
            "open_interest": [41],
        }
    )
    published_zeros = stale.with_columns(
        pl.Series(
            "timestamp",
            [datetime(2026, 10, 27, 6, 30, tzinfo=eastern)],
        ),
        pl.lit(0).alias("open_interest"),
    )
    assert open_interest_covers_session(None, trade) is False
    assert open_interest_covers_session(pl.DataFrame(), trade) is False
    assert open_interest_covers_session(stale, trade) is False
    assert open_interest_covers_session(published_zeros, trade) is True
    assert open_interest_probe_symbol({}) == "AAPL"
    assert (
        open_interest_probe_symbol({"AAPL": 10, "SPY": 50, "QQQ": 50}) == "QQQ"
    )


def test_option_frame_parses_string_expirations() -> None:
    trade = date(2026, 10, 23)
    eastern = ZoneInfo("America/New_York")
    end_of_day = pl.DataFrame(
        {
            "symbol": ["AAPL"],
            "expiration": ["2026-11-20"],
            "strike": [150.0],
            "right": ["CALL"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.05],
            "volume": [10],
            "created": [datetime(2026, 10, 23, 16, 15, tzinfo=eastern)],
        }
    )
    interest = pl.DataFrame(
        {
            "symbol": ["AAPL"],
            "expiration": ["2026-11-20"],
            "strike": [150.0],
            "right": ["CALL"],
            "timestamp": [datetime(2026, 10, 26, 6, 30, tzinfo=eastern)],
            "open_interest": [41],
        }
    )
    frame = build_option_frame(end_of_day, interest, trade, "AAPL")
    assert frame["expiration"].to_list() == [date(2026, 11, 20)]
    assert frame["right"].to_list() == ["call"]
    assert frame["open_interest"].to_list() == [41]
    assert frame["date"].to_list() == [trade]


def test_option_csv_uses_the_trade_date_and_utc_timestamps() -> None:
    trade = date(2026, 10, 23)
    frame = build_option_frame(_sample_eod(), None, trade, "AAPL")
    body = frame_to_csv(frame)
    text = body.decode()
    assert text.splitlines()[0] == ",".join(OPTION_COLUMNS)
    assert "2026-10-23T20:15:00.000000" in text
    assert "2026-10-23T20:00:00.000000" in text
    assert "2026-10-23T19:59:00.000000" in text
    assert ",2026-10-23\n" in text
    combined = concat_checkpoint_csvs([frame_to_csv(empty_option_frame()), body])
    lines = combined.decode().splitlines()
    assert lines[0] == ",".join(OPTION_COLUMNS)
    assert len(lines) == frame.height + 1


class _RecordingQuack:
    def __init__(self, selects: list[list[tuple]], scalars: list[int]) -> None:
        self.selects = selects
        self.scalars = scalars
        self.statements: list[str] = []

    def execute(self, sql: str) -> list[tuple]:
        self.statements.append(sql)
        if sql.lstrip().startswith("SELECT"):
            return self.selects.pop(0)
        return []

    def scalar(self, sql: str) -> int:
        self.statements.append(sql)
        return self.scalars.pop(0)


def test_option_append_sql_skips_symbols_already_loaded() -> None:
    part = "securities/option_data/2026-10-26/part-0000.csv"
    data_sql = create_option_data_sql()
    assert "CREATE TABLE IF NOT EXISTS securities_data.securities.option_data" in data_sql
    assert '"created" TIMESTAMP' in data_sql
    assert '"right" VARCHAR' in data_sql
    assert "TIMESTAMPTZ" not in data_sql
    assert '"open_interest" BIGINT' in data_sql
    volume_sql = create_option_volume_sql()
    assert OPTION_VOLUME_TABLE in volume_sql
    assert '"total_volume" BIGINT' in volume_sql
    plain = option_insert_sql(part, [])
    assert "WHERE" not in plain
    assert '"symbol", "expiration", "strike", "right"' in plain
    assert "getenv" not in plain
    assert 'GROUP BY "symbol", "date"' in volume_insert_sql(part, [])
    assert "date < DATE '2026-10-26'" in latest_volume_sql("2026-10-26")
    assert "row_number()" in latest_volume_sql("2026-10-26")
    with pytest.raises(ValueError):
        symbols_present_sql(OPTION_DATA_TABLE, "2026-10-26", ["AAPL'; DROP"])
    with pytest.raises(ValueError):
        symbols_present_sql("securities_data.securities.stocks", "2026-10-26", ["AAPL"])

    client = _RecordingQuack(
        selects=[[("AAPL",)], [("AAPL",)]],
        scalars=[10, 12],
    )
    appended = append_option_part(
        client,
        "http://192.168.0.20:8181/catalog",
        "2026-10-26",
        part,
        ["AAPL", "MSFT"],
    )
    assert appended == 2
    inserts = [sql for sql in client.statements if sql.startswith("INSERT INTO")]
    assert len(inserts) == 2
    assert all("NOT IN ('AAPL')" in sql for sql in inserts)
    assert all("MSFT" not in sql.split("WHERE", 1)[-1] for sql in inserts)
    assert any(OPTION_DATA_TABLE in sql for sql in inserts)
    assert any(OPTION_VOLUME_TABLE in sql for sql in inserts)
    assert all("getenv" not in sql for sql in inserts)

    loaded = _RecordingQuack(
        selects=[[("AAPL",), ("MSFT",)], [("AAPL",), ("MSFT",)]],
        scalars=[4, 4],
    )
    assert (
        append_option_part(
            loaded,
            "http://192.168.0.20:8181/catalog",
            "2026-10-26",
            part,
            ["AAPL", "MSFT"],
        )
        == 0
    )
    assert not any(sql.startswith("INSERT INTO") for sql in loaded.statements)


class _Gate:
    def __init__(self) -> None:
        self.jobs: list[str] = []

    def lease(self, job_name: str, heartbeat=None):
        self.jobs.append(job_name)
        return nullcontext("lease")


class _ChainClient:
    def __init__(self, end_of_day, open_interest=None, *, oi_missing: bool = False) -> None:
        self.end_of_day = end_of_day
        self.open_interest = open_interest
        self.oi_missing = oi_missing
        self.calls: list[str] = []

    def option_history_eod(self, start_date, end_date, symbol, expiration):
        self.calls.append("eod")
        if isinstance(self.end_of_day, Exception):
            raise self.end_of_day
        return self.end_of_day

    def option_history_open_interest(
        self, symbol, expiration, date=None, start_date=None, end_date=None
    ):
        self.calls.append("oi")
        if self.oi_missing:
            raise NoDataFoundError("missing")
        return self.open_interest

    def option_snapshot_open_interest(self, symbol, expiration, current_day_only=False):
        self.calls.append("snap")
        if self.oi_missing:
            raise NoDataFoundError("missing")
        return self.open_interest


def test_missing_open_interest_writes_zeros_without_failing() -> None:
    trade = date(2026, 10, 23)
    gate = _Gate()
    empty = _ChainClient(NoDataFoundError("none"))
    frame = load_option_chain(
        empty, gate, "AAPL", trade, date(2026, 10, 26), as_of=date(2026, 10, 27)
    )
    assert frame.height == 0
    assert empty.calls == ["eod"]
    assert gate.jobs == ["ThetaDataOptionDataWorkflow"]

    blank = _ChainClient(pl.DataFrame())
    blank_gate = _Gate()
    assert (
        load_option_chain(
            blank, blank_gate, "AAPL", trade, date(2026, 10, 26), as_of=date(2026, 10, 27)
        ).height
        == 0
    )
    assert blank.calls == ["eod"]

    gate = _Gate()
    missing_oi = _ChainClient(_sample_eod(), oi_missing=True)
    filled = load_option_chain(
        missing_oi, gate, "AAPL", trade, date(2026, 10, 26), as_of=date(2026, 10, 27)
    )
    assert filled["open_interest"].to_list() == [0, 0, 0]
    assert missing_oi.calls == ["eod", "oi"]
    assert gate.jobs == ["ThetaDataOptionDataWorkflow", "ThetaDataOptionDataWorkflow"]

    current = _ChainClient(_sample_eod())
    load_option_chain(
        current, _Gate(), "AAPL", trade, date(2026, 10, 26), as_of=date(2026, 10, 26)
    )
    assert current.calls == ["eod", "snap"]


def _required_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THETADATA_API_KEY", "td")
    monkeypatch.setenv("GARAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("GARAGE_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("QUACK_TOKEN", "token")
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/0")
    for name in (
        "THETADATA_JOB_PRIORITIES",
        "THETADATA_MAX_CONCURRENT",
        "THETADATA_REQUESTS_PER_MINUTE",
        "THETADATA_LEASE_SECONDS",
        "THETADATA_OPTION_BATCH_SIZE",
        "THETADATA_OPTION_MIN_ROWS",
    ):
        monkeypatch.delenv(name, raising=False)


def test_settings_require_redis_and_reject_bad_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _required_settings(monkeypatch)
    monkeypatch.delenv("REDIS_URL", raising=False)
    with pytest.raises(RuntimeError, match="REDIS_URL"):
        Settings.from_env()

    _required_settings(monkeypatch)
    monkeypatch.setenv("REDIS_URL", "http://redis:6379/0")
    with pytest.raises(RuntimeError, match="REDIS_URL"):
        Settings.from_env()

    _required_settings(monkeypatch)
    monkeypatch.setenv("THETADATA_JOB_PRIORITIES", "nope")
    with pytest.raises(RuntimeError, match="THETADATA_JOB_PRIORITIES"):
        Settings.from_env()
    monkeypatch.setenv("THETADATA_JOB_PRIORITIES", "ThetaDataOptionDataWorkflow=-1")
    with pytest.raises(RuntimeError, match="THETADATA_JOB_PRIORITIES"):
        Settings.from_env()

    _required_settings(monkeypatch)
    monkeypatch.setenv("THETADATA_JOB_PRIORITIES", "ThetaDataOptionDataWorkflow=3")
    settings = Settings.from_env()
    assert settings.thetadata_job_priorities["ThetaDataOptionDataWorkflow"] == 3
    assert (
        settings.thetadata_job_priorities["ThetaDataSecuritiesWorkflow"]
        == DEFAULT_JOB_PRIORITIES["ThetaDataSecuritiesWorkflow"]
    )
    assert settings.thetadata_max_concurrent == 2
    assert settings.thetadata_requests_per_minute == 0

    monkeypatch.setenv("THETADATA_MAX_CONCURRENT", "0")
    with pytest.raises(RuntimeError, match="THETADATA_MAX_CONCURRENT"):
        Settings.from_env()
    monkeypatch.setenv("THETADATA_MAX_CONCURRENT", "2")
