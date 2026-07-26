from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NewMarket:
    ticker: str
    event_ticker: str
    series_ticker: str
    title: str
    subtitle: str | None
    category: str
    created_time: str
    close_time: str | None


@dataclass(frozen=True)
class MarketFetchResult:
    markets: list[NewMarket]
    categories: list[str]


@dataclass(frozen=True)
class PollRequest:
    workflow_id: str
    workflow_run_id: str
    observed_at: str


@dataclass(frozen=True)
class StageResult:
    staged_workflow_run_id: str
    notification_batches: list[str]
    next_batch_index: int
    new_market_count: int
    new_category_count: int
    initialized: bool
    pending_existing: bool
    discord_batch_delay_seconds: float


@dataclass(frozen=True)
class MarkBatchRequest:
    workflow_run_id: str
    batch_index: int


@dataclass(frozen=True)
class WorkflowResult:
    workflow_run_id: str
    new_market_count: int
    new_category_count: int
    notification_batches_sent: int
    initialized: bool
