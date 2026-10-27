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
