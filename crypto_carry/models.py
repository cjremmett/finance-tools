from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SpotPrice:
    asset: str
    price: str
    source: str
    source_url: str


@dataclass(frozen=True)
class FuturesQuote:
    asset: str
    symbol: str
    contract_code: str
    month_label: str
    expiration: str
    last_trade_at: str
    price: str


@dataclass(frozen=True)
class CarryRow:
    asset: str
    contract_code: str
    month_label: str
    expiration: str
    days_remaining: str
    futures_price: str
    spot_price: str
    apy_percent: str
    direction: str


@dataclass(frozen=True)
class CarrySnapshot:
    observed_at: str
    spots: list[SpotPrice]
    rows: list[CarryRow]


@dataclass(frozen=True)
class FetchRequest:
    observed_at: str


@dataclass(frozen=True)
class WorkflowResult:
    workflow_run_id: str
    btc_contract_count: int
    eth_contract_count: int
    notification_batches_sent: int
