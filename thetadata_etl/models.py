from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SymbolSnapshotRequest:
    dataset: str
    fetch_date: str


@dataclass(frozen=True)
class AppendResult:
    appended: bool
    row_count: int


@dataclass(frozen=True)
class DatasetResult:
    dataset: str
    fetched: bool
    appended: bool
    row_count: int


@dataclass(frozen=True)
class WorkflowResult:
    workflow_run_id: str
    fetch_date: str
    datasets: list[DatasetResult]


@dataclass(frozen=True)
class SessionResolution:
    trade_date: str
    oi_end_date: str
    skipped: bool
    skip_reason: str


@dataclass(frozen=True)
class LoadedDay:
    loaded: bool
    row_count: int


@dataclass(frozen=True)
class OptionPlan:
    symbol_snapshot_date: str
    underlying_count: int
    batches: list[list[str]]


@dataclass(frozen=True)
class OptionBatch:
    trade_date: str
    oi_end_date: str
    batch_index: int
    symbols: list[str]


@dataclass(frozen=True)
class BatchFetchResult:
    fetched: int


@dataclass(frozen=True)
class BatchAppendResult:
    appended_rows: int


@dataclass(frozen=True)
class OptionWorkflowResult:
    workflow_run_id: str
    trade_date: str
    skipped: bool
    skip_reason: str
    symbol_snapshot_date: str
    underlying_count: int
    fetched_count: int
    appended_rows: int
    row_count: int
