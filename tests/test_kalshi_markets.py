from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from kalshi_markets.config import Settings
from kalshi_markets.database import (
    Base,
    KNOWN_CATEGORY_NAMES,
    KnownCategory,
    LatestMarket,
    MonitorState,
    get_engine,
    mark_batch_sent,
    poll_and_stage,
)
from kalshi_markets.discord import (
    DISCORD_CONTENT_LIMIT,
    build_failure_message,
    build_market_messages,
    build_new_category_messages,
)
from kalshi_markets.kalshi import KalshiClient, _meets_minimum_duration
from kalshi_markets.models import (
    MarkBatchRequest,
    MarketFetchResult,
    NewMarket,
    PollRequest,
)
from kalshi_markets.workflows import KalshiMarketMonitorWorkflow


def _settings(database_url: str) -> Settings:
    return Settings(
        temporal_address="localhost:7233",
        temporal_namespace="default",
        temporal_task_queue="kalshi-markets",
        database_url=database_url,
        discord_webhook_url="https://discord.test/webhook",
        kalshi_api_key_id="key-id",
        kalshi_private_key_base64="unused-by-fake-client",
        category_whitelist=("Economics", "Science and Technology"),
        http_timeout_seconds=20,
        discord_batch_delay_seconds=0,
        kalshi_requests_per_second=10,
        kalshi_event_resolution_timeout_seconds=60,
        minimum_market_duration_days=0,
    )


def _market(
    ticker: str,
    *,
    title: str = "Will something happen?",
    subtitle: str | None = None,
    category: str = "Economics",
    created_time: str = "2026-07-26T12:00:00Z",
) -> NewMarket:
    return NewMarket(
        ticker=ticker,
        event_ticker="EVENT-1",
        series_ticker="SERIES-1",
        title=title,
        subtitle=subtitle,
        category=category,
        created_time=created_time,
        close_time="2027-01-01T00:00:00Z",
    )


class FakeClient:
    def __init__(
        self,
        markets: list[NewMarket],
        categories: list[str] | None = None,
    ):
        self.markets = markets
        self.categories = categories or list(KNOWN_CATEGORY_NAMES)
        self.windows: list[tuple[datetime, datetime]] = []

    def fetch_categories(self) -> list[str]:
        return self.categories

    def fetch_new_markets(
        self, window_start: datetime, window_end: datetime
    ) -> MarketFetchResult:
        self.windows.append((window_start, window_end))
        return MarketFetchResult(
            markets=self.markets,
            categories=self.categories,
        )


def _request(run_id: str, timestamp: str) -> PollRequest:
    return PollRequest(
        workflow_id=f"workflow-{run_id}",
        workflow_run_id=run_id,
        observed_at=timestamp,
    )


def test_database_seeds_then_replaces_only_latest_window(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'kalshi.db'}"
    get_engine.cache_clear()
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)
    settings = _settings(database_url)

    initialized = poll_and_stage(
        _request("run-1", "2026-07-26T10:00:00Z"),
        database_url=database_url,
        settings=settings,
        client=FakeClient([]),
    )
    assert initialized.initialized is True
    assert initialized.new_market_count == 0
    assert "initialized" in initialized.notification_batches[0]
    mark_batch_sent(
        MarkBatchRequest(
            workflow_run_id=initialized.staged_workflow_run_id,
            batch_index=0,
        ),
        database_url,
    )

    aliens = _market(
        "KXALIENS-26AUG",
        title="Will the U.S. confirm that aliens exist?",
        subtitle="Before August",
        category="Science and Technology",
    )
    changed_client = FakeClient([aliens])
    changed = poll_and_stage(
        _request("run-2", "2026-07-27T10:00:00Z"),
        database_url=database_url,
        settings=settings,
        client=changed_client,
    )
    assert changed.new_market_count == 1
    assert "Before August" in changed.notification_batches[0]
    changed_start, changed_end = changed_client.windows[0]
    assert changed_start.replace(tzinfo=timezone.utc) == datetime(
        2026, 7, 26, 10, tzinfo=timezone.utc
    )
    assert changed_end == datetime(2026, 7, 27, 10, tzinfo=timezone.utc)
    mark_batch_sent(
        MarkBatchRequest(
            workflow_run_id=changed.staged_workflow_run_id,
            batch_index=0,
        ),
        database_url,
    )

    unchanged_client = FakeClient([])
    unchanged = poll_and_stage(
        _request("run-3", "2026-07-28T10:00:00Z"),
        database_url=database_url,
        settings=settings,
        client=unchanged_client,
    )
    assert unchanged.new_market_count == 0
    assert "No new markets" in unchanged.notification_batches[0]
    unchanged_start, unchanged_end = unchanged_client.windows[0]
    assert unchanged_start.replace(tzinfo=timezone.utc) == datetime(
        2026, 7, 27, 10, tzinfo=timezone.utc
    )
    assert unchanged_end == datetime(2026, 7, 28, 10, tzinfo=timezone.utc)
    with Session(engine) as session:
        assert session.scalars(select(LatestMarket)).all() == []
        state = session.get(MonitorState, 1)
        assert state is not None
        assert state.workflow_run_id == "run-3"


def test_pending_batches_are_returned_without_advancing_cutoff(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'pending.db'}"
    get_engine.cache_clear()
    Base.metadata.create_all(get_engine(database_url))
    settings = _settings(database_url)

    first = poll_and_stage(
        _request("run-1", "2026-07-26T10:00:00Z"),
        database_url=database_url,
        settings=settings,
        client=FakeClient([]),
    )
    pending = poll_and_stage(
        _request("run-2", "2026-07-27T10:00:00Z"),
        database_url=database_url,
        settings=settings,
        client=FakeClient([_market("NEW")]),
    )
    assert pending.pending_existing is True
    assert pending.staged_workflow_run_id == first.staged_workflow_run_id
    assert pending.notification_batches == first.notification_batches


def test_market_messages_batch_and_group_categories() -> None:
    markets = [
        _market(
            f"ECON-{index}",
            title=f"Economic market {index} " + ("x" * 120),
        )
        for index in range(30)
    ]
    markets.append(
        _market(
            "ALIENS-AUG",
            title="Will aliens be confirmed?",
            subtitle="Before August",
            category="Science and Technology",
        )
    )

    messages = build_market_messages(markets)

    assert len(messages) > 1
    assert all(len(message) <= DISCORD_CONTENT_LIMIT for message in messages)
    assert sum("Economic market" in message for message in messages) > 1
    assert any("**Economics**" in message for message in messages)
    assert any("Before August" in message for message in messages)

    oversized = build_market_messages(
        [_market("LONG", title="A" * (DISCORD_CONTENT_LIMIT * 2))]
    )
    assert len(oversized) == 1
    assert len(oversized[0]) <= DISCORD_CONTENT_LIMIT
    assert "…" in oversized[0]


def test_new_category_is_stored_and_sent_as_a_separate_message(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'categories.db'}"
    get_engine.cache_clear()
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)
    settings = _settings(database_url)

    initialized = poll_and_stage(
        _request("run-1", "2026-07-26T10:00:00Z"),
        database_url=database_url,
        settings=settings,
        client=FakeClient(
            [],
            categories=[*KNOWN_CATEGORY_NAMES, "Artificial Intelligence"],
        ),
    )

    assert initialized.new_category_count == 1
    assert len(initialized.notification_batches) == 2
    assert "initialized" in initialized.notification_batches[0]
    assert "New Kalshi categories" in initialized.notification_batches[1]
    assert "Artificial Intelligence" in initialized.notification_batches[1]
    with Session(engine) as session:
        categories = set(
            session.scalars(select(KnownCategory.category)).all()
        )
    assert categories == {*KNOWN_CATEGORY_NAMES, "Artificial Intelligence"}


def test_new_category_messages_are_bounded() -> None:
    messages = build_new_category_messages(
        [f"Category {index} " + "x" * 100 for index in range(30)]
    )
    assert len(messages) > 1
    assert all(len(message) <= DISCORD_CONTENT_LIMIT for message in messages)


def test_failure_message_is_sanitized_and_bounded() -> None:
    message = build_failure_message("run-1", "polling", "secret\n" + "x" * 3000)
    assert "failed after retries" in message
    assert "\nxxxx" not in message
    assert len(message) <= DISCORD_CONTENT_LIMIT


def test_kalshi_client_retries_event_resolution_without_refetching_markets(
    monkeypatch,
) -> None:
    settings = _settings("sqlite://")
    client = object.__new__(KalshiClient)
    client.settings = settings
    calls: list[tuple[str, dict[str, Any]]] = []
    event_attempts = 0
    sleep_delays: list[float] = []

    def paginate(
        path: str, item_key: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        nonlocal event_attempts
        calls.append((path, params))
        if path == "/markets":
            return [
                {
                    "ticker": "KXALIENS-26AUG",
                    "event_ticker": "KXALIENS-27",
                    "title": "Will aliens be confirmed?",
                    "yes_sub_title": "Before August",
                    "created_time": "2026-07-26T12:00:00.500Z",
                    "close_time": "2026-08-01T14:00:00Z",
                },
                {
                    "ticker": "TOO-OLD",
                    "event_ticker": "KXALIENS-27",
                    "title": "Old",
                    "created_time": "2026-07-26T10:00:00Z",
                },
            ]
        assert item_key == "events"
        event_attempts += 1
        if event_attempts <= 3:
            return []
        return [
            {
                "event_ticker": "KXALIENS-27",
                "series_ticker": "KXALIENS",
            }
        ]

    def get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if path == "/search/tags_by_categories":
            return {
                "tags_by_categories": {
                    "Economics": [],
                    "Science and Technology": ["Space"],
                }
            }
        if path == "/series":
            return {
                "series": [
                    {
                        "ticker": "KXALIENS",
                        "category": "Science and Technology",
                    }
                ]
            }
        raise AssertionError(path)

    monkeypatch.setattr(
        "kalshi_markets.kalshi.time.sleep", sleep_delays.append
    )
    client._paginate = paginate
    client._get = get
    result = client.fetch_new_markets(
        datetime(2026, 7, 26, 10, tzinfo=timezone.utc),
        datetime(2026, 7, 26, 13, tzinfo=timezone.utc),
    )

    assert [market.ticker for market in result.markets] == ["KXALIENS-26AUG"]
    assert result.categories == ["Economics", "Science and Technology"]
    assert calls[0][0] == "/markets"
    assert calls[0][1]["mve_filter"] == "exclude"
    assert sum(path == "/markets" for path, _ in calls) == 1
    assert sum(path == "/events" for path, _ in calls) == 4
    assert sleep_delays == [5.0, 10.0, 20.0]


def test_kalshi_client_ignores_markets_with_events_unresolved_at_timeout(
    monkeypatch, caplog
) -> None:
    settings = _settings("sqlite://")
    client = object.__new__(KalshiClient)
    client.settings = settings

    def paginate(
        path: str, item_key: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if path == "/markets":
            return [
                {
                    "ticker": "RESOLVED-MARKET",
                    "event_ticker": "RESOLVED-EVENT",
                    "created_time": "2026-07-26T12:00:00Z",
                },
                {
                    "ticker": "UNRESOLVED-MARKET",
                    "event_ticker": "UNRESOLVED-EVENT",
                    "created_time": "2026-07-26T12:00:01Z",
                },
            ]
        assert path == "/events"
        return [
            {
                "event_ticker": "RESOLVED-EVENT",
                "series_ticker": "RESOLVED-SERIES",
            }
        ]

    def get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if path == "/search/tags_by_categories":
            return {
                "tags_by_categories": {
                    "Economics": [],
                    "Science and Technology": [],
                }
            }
        if path == "/series":
            return {
                "series": [
                    {
                        "ticker": "RESOLVED-SERIES",
                        "category": "Economics",
                    }
                ]
            }
        raise AssertionError(path)

    monotonic_values = iter((0.0, 60.0))
    monkeypatch.setattr(
        "kalshi_markets.kalshi.time.monotonic",
        lambda: next(monotonic_values),
    )
    client._paginate = paginate
    client._get = get

    result = client.fetch_new_markets(
        datetime(2026, 7, 26, 10, tzinfo=timezone.utc),
        datetime(2026, 7, 26, 13, tzinfo=timezone.utc),
    )

    assert [market.ticker for market in result.markets] == ["RESOLVED-MARKET"]
    assert "ignoring their markets: UNRESOLVED-EVENT" in caplog.text


def test_minimum_market_duration_boundaries_and_invalid_data() -> None:
    base = {
        "ticker": "DURATION",
        "open_time": "2026-07-26T12:00:00Z",
    }
    assert not _meets_minimum_duration(
        {**base, "close_time": "2026-07-27T11:59:59Z"}, 1
    )
    assert _meets_minimum_duration(
        {**base, "close_time": "2026-07-27T12:00:00Z"}, 1
    )
    assert _meets_minimum_duration(
        {**base, "close_time": "2026-07-27T00:00:00Z"}, 0.5
    )
    assert _meets_minimum_duration({"ticker": "MISSING"}, 1)
    assert _meets_minimum_duration(
        {**base, "close_time": "not-a-timestamp"}, 1
    )
    assert _meets_minimum_duration(
        {**base, "close_time": "2026-07-26T11:59:59Z"}, 1
    )


def test_minimum_market_duration_configuration(monkeypatch) -> None:
    required_environment = {
        "KALSHI_CATEGORY_WHITELIST": "Economics",
        "KALSHI_DISCORD_WEBHOOK_URL": "https://discord.test/webhook",
        "KALSHI_API_KEY_ID": "key-id",
        "KALSHI_PRIVATE_KEY_BASE64": "key",
    }
    for name, value in required_environment.items():
        monkeypatch.setenv(name, value)

    monkeypatch.delenv("KALSHI_EVENT_RESOLUTION_TIMEOUT_SECONDS", raising=False)
    assert Settings.from_env().kalshi_event_resolution_timeout_seconds == 60

    monkeypatch.setenv("KALSHI_MIN_MARKET_DURATION_DAYS", "1.5")
    assert Settings.from_env().minimum_market_duration_days == 1.5

    for invalid_value in ("-1", "nan", "not-a-number"):
        monkeypatch.setenv(
            "KALSHI_MIN_MARKET_DURATION_DAYS", invalid_value
        )
        with pytest.raises(
            RuntimeError,
            match="KALSHI_MIN_MARKET_DURATION_DAYS must be a non-negative number",
        ):
            Settings.from_env()


def test_duration_filter_avoids_event_and_series_requests() -> None:
    settings = replace(_settings("sqlite://"), minimum_market_duration_days=2)
    client = object.__new__(KalshiClient)
    client.settings = settings
    calls: list[str] = []

    def paginate(
        path: str, item_key: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        calls.append(path)
        assert path == "/markets"
        return [
            {
                "ticker": "SHORT",
                "event_ticker": "SHORT-EVENT",
                "created_time": "2026-07-26T12:00:00Z",
                "open_time": "2026-07-26T12:00:00Z",
                "close_time": "2026-07-27T12:00:00Z",
            }
        ]

    def get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        calls.append(path)
        assert path == "/search/tags_by_categories"
        return {
            "tags_by_categories": {
                "Economics": [],
                "Science and Technology": [],
            }
        }

    client._paginate = paginate
    client._get = get
    result = client.fetch_new_markets(
        datetime(2026, 7, 26, 10, tzinfo=timezone.utc),
        datetime(2026, 7, 26, 13, tzinfo=timezone.utc),
    )

    assert result.markets == []
    assert calls == ["/markets", "/search/tags_by_categories"]


def test_kalshi_workflow_passes_temporal_sandbox_validation() -> None:
    async def validate() -> None:
        SandboxedWorkflowRunner().prepare_workflow(
            workflow._Definition.must_from_class(KalshiMarketMonitorWorkflow)
        )

    asyncio.run(validate())
