from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    MetaData,
    String,
    Text,
    create_engine,
    delete,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from kalshi_markets.config import Settings
from kalshi_markets.discord import (
    build_initialization_message,
    build_market_messages,
    build_new_category_messages,
)
from kalshi_markets.kalshi import KalshiClient
from kalshi_markets.models import (
    MarkBatchRequest,
    MarketFetchResult,
    NewMarket,
    PollRequest,
    StageResult,
)

DB_SCHEMA = "kalshi_markets"
STATE_ID = 1
KNOWN_CATEGORY_NAMES = (
    "Climate and Weather",
    "Commodities",
    "Companies",
    "Crypto",
    "Economics",
    "Elections",
    "Entertainment",
    "Financials",
    "Mentions",
    "Politics",
    "Science and Technology",
    "Social",
    "Sports",
)


class Base(DeclarativeBase):
    metadata = MetaData(schema=DB_SCHEMA)


class MonitorState(Base):
    __tablename__ = "monitor_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    last_successful_cutoff: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    window_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    window_ended_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    notification_batches: Mapped[list[str]] = mapped_column(
        JSON, nullable=False, default=list
    )
    next_batch_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    notification_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending"
    )
    last_error: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class LatestMarket(Base):
    __tablename__ = "latest_markets"

    ticker: Mapped[str] = mapped_column(String(255), primary_key=True)
    event_ticker: Mapped[str] = mapped_column(String(255), nullable=False)
    series_ticker: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    subtitle: Mapped[str | None] = mapped_column(Text)
    category: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    created_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    close_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnownCategory(Base):
    __tablename__ = "known_categories"

    category: Mapped[str] = mapped_column(String(255), primary_key=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


@lru_cache(maxsize=4)
def get_engine(database_url: str):
    engine = create_engine(database_url, pool_pre_ping=True)
    if engine.dialect.name == "sqlite":
        engine = engine.execution_options(schema_translate_map={DB_SCHEMA: None})
    return engine


def _as_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _stage_result(
    state: MonitorState,
    *,
    new_market_count: int,
    new_category_count: int,
    initialized: bool,
    pending_existing: bool,
    delay: float,
) -> StageResult:
    return StageResult(
        staged_workflow_run_id=state.workflow_run_id,
        notification_batches=list(state.notification_batches),
        next_batch_index=state.next_batch_index,
        new_market_count=new_market_count,
        new_category_count=new_category_count,
        initialized=initialized,
        pending_existing=pending_existing,
        discord_batch_delay_seconds=delay,
    )


def poll_and_stage(
    request: PollRequest,
    *,
    database_url: str | None = None,
    settings: Settings | None = None,
    client: KalshiClient | None = None,
) -> StageResult:
    settings = settings or Settings.from_env()
    database_url = database_url or settings.database_url
    engine = get_engine(database_url)
    observed_at = _as_datetime(request.observed_at)

    with Session(engine) as session:
        state = session.get(MonitorState, STATE_ID)
        known_categories = set(
            session.scalars(select(KnownCategory.category)).all()
        ) | set(KNOWN_CATEGORY_NAMES)
        if state is not None and state.notification_status == "pending":
            return _stage_result(
                state,
                new_market_count=len(session.scalars(select(LatestMarket)).all()),
                new_category_count=0,
                initialized=False,
                pending_existing=True,
                delay=settings.discord_batch_delay_seconds,
            )
        previous_cutoff = state.last_successful_cutoff if state else None

    if previous_cutoff is None:
        client = client or KalshiClient(settings)
        observed_categories = client.fetch_categories()
        markets: list[NewMarket] = []
        batches = [build_initialization_message()]
        initialized = True
    else:
        client = client or KalshiClient(settings)
        fetched: MarketFetchResult = client.fetch_new_markets(
            previous_cutoff, observed_at
        )
        markets = fetched.markets
        observed_categories = fetched.categories
        batches = build_market_messages(markets)
        initialized = False
    new_categories = sorted(set(observed_categories) - known_categories)
    batches.extend(build_new_category_messages(new_categories))

    with Session(engine) as session, session.begin():
        if engine.dialect.name == "postgresql":
            session.execute(
                text(
                    "SELECT pg_advisory_xact_lock("
                    "hashtext('finance_tools_kalshi_market_persistence'))"
                )
            )
        current = session.get(MonitorState, STATE_ID)
        current_cutoff = current.last_successful_cutoff if current else None
        if current is not None and current.notification_status == "pending":
            return _stage_result(
                current,
                new_market_count=len(
                    session.scalars(select(LatestMarket)).all()
                ),
                new_category_count=0,
                initialized=False,
                pending_existing=True,
                delay=settings.discord_batch_delay_seconds,
            )
        if current_cutoff != previous_cutoff:
            raise RuntimeError("Kalshi monitor cutoff changed concurrently")

        session.execute(delete(LatestMarket))
        existing_categories = set(
            session.scalars(select(KnownCategory.category)).all()
        )
        for category in sorted(
            set(KNOWN_CATEGORY_NAMES) | set(observed_categories)
        ):
            if category not in existing_categories:
                session.add(
                    KnownCategory(
                        category=category,
                        first_seen_at=observed_at,
                    )
                )
        for market in markets:
            session.add(
                LatestMarket(
                    ticker=market.ticker,
                    event_ticker=market.event_ticker,
                    series_ticker=market.series_ticker,
                    title=market.title,
                    subtitle=market.subtitle,
                    category=market.category,
                    created_time=_as_datetime(market.created_time),
                    close_time=(
                        _as_datetime(market.close_time) if market.close_time else None
                    ),
                )
            )

        values: dict[str, Any] = {
            "last_successful_cutoff": observed_at,
            "workflow_id": request.workflow_id,
            "workflow_run_id": request.workflow_run_id,
            "window_started_at": previous_cutoff,
            "window_ended_at": observed_at,
            "notification_batches": batches,
            "next_batch_index": 0,
            "notification_status": "pending",
            "last_error": None,
            "updated_at": datetime.now(timezone.utc),
        }
        if current is None:
            current = MonitorState(id=STATE_ID, **values)
            session.add(current)
        else:
            for key, value in values.items():
                setattr(current, key, value)
        session.flush()
        return _stage_result(
            current,
            new_market_count=len(markets),
            new_category_count=len(new_categories),
            initialized=initialized,
            pending_existing=False,
            delay=settings.discord_batch_delay_seconds,
        )


def mark_batch_sent(
    request: MarkBatchRequest, database_url: str | None = None
) -> None:
    database_url = database_url or Settings.from_env().database_url
    engine = get_engine(database_url)
    with Session(engine) as session, session.begin():
        if engine.dialect.name == "postgresql":
            session.execute(
                text(
                    "SELECT pg_advisory_xact_lock("
                    "hashtext('finance_tools_kalshi_market_persistence'))"
                )
            )
        state = session.get(MonitorState, STATE_ID)
        if state is None or state.workflow_run_id != request.workflow_run_id:
            raise RuntimeError("notification state does not match this workflow run")
        if request.batch_index < state.next_batch_index:
            return
        if request.batch_index != state.next_batch_index:
            raise RuntimeError("notification batches must be acknowledged in order")
        state.next_batch_index += 1
        if state.next_batch_index >= len(state.notification_batches):
            state.notification_status = "sent"
        state.updated_at = datetime.now(timezone.utc)
