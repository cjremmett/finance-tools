import asyncio
from datetime import date, datetime, timezone

import polars as pl
import pytest
from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from thetadata_etl.catalog import (
    SCHEMA_SQL,
    SECRET_SQL,
    append_csv,
    attach_sql,
    count_sql,
    create_table_sql,
    insert_sql,
)
from thetadata_etl.config import Settings
from thetadata_etl.datasets import (
    eastern_fetch_date,
    object_key,
    require_catalog_uri,
    require_fetch_date,
)
from thetadata_etl.symbols import build_symbol_csv
from thetadata_etl.workflows import ThetaDataSecuritiesWorkflow


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
        SandboxedWorkflowRunner().prepare_workflow(
            workflow._Definition.must_from_class(ThetaDataSecuritiesWorkflow)
        )

    asyncio.run(validate())
