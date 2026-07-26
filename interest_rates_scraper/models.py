from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RateObservation:
    source: str
    product_key: str
    product_name: str
    product_type: str
    rate_percent: str
    source_url: str
    term_months: int | None = None
    source_effective_date: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ScrapeResult:
    source: str
    observations: list[RateObservation]
    source_effective_date: str | None = None


@dataclass(frozen=True)
class SourceFailure:
    source: str
    error: str


@dataclass(frozen=True)
class PersistRequest:
    workflow_id: str
    workflow_run_id: str
    observed_at: str
    results: list[ScrapeResult]
    failures: list[SourceFailure]


@dataclass(frozen=True)
class RateChange:
    source: str
    product_key: str
    product_name: str
    change_type: str
    old_rate_percent: str | None
    new_rate_percent: str | None


@dataclass(frozen=True)
class PersistResult:
    changes: list[RateChange]
    baseline: bool


@dataclass(frozen=True)
class DiscordNotification:
    workflow_run_id: str
    changes: list[RateChange]
    failures: list[SourceFailure]
    baseline: bool


@dataclass(frozen=True)
class WorkflowResult:
    workflow_run_id: str
    successful_sources: list[str]
    failed_sources: list[str]
    changes: int
    notification_sent: bool
