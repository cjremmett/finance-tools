from __future__ import annotations

import os

import requests
from temporalio.exceptions import ActivityError

from marketparquet_etl.config import Settings
from marketparquet_etl.models import (
    AssetState,
    StockPlan,
    WorkflowResult,
    append_batches,
    fetch_batches,
    require_asset,
    window_bounds,
)

DISCORD_CONTENT_LIMIT = 2000
_GENERIC_ACTIVITY_FAILURE = "Activity task failed"


def _clip(content: str) -> str:
    if len(content) <= DISCORD_CONTENT_LIMIT:
        return content
    return content[: DISCORD_CONTENT_LIMIT - 1] + "…"


def _next_cause(current: BaseException) -> BaseException | None:
    cause = current.__cause__
    if isinstance(cause, BaseException) and cause is not current:
        return cause
    cause = getattr(current, "cause", None)
    if isinstance(cause, BaseException) and cause is not current:
        return cause
    context = current.__context__
    if isinstance(context, BaseException) and context is not current:
        return context
    return None


def failure_reason(error: BaseException) -> str:
    activity_type = ""
    messages: list[str] = []
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ActivityError) and not activity_type:
            activity_type = current.activity_type
        text = " ".join(str(current).split())
        if text and text != _GENERIC_ACTIVITY_FAILURE and text not in messages:
            messages.append(text)
        current = _next_cause(current)
    reason = messages[-1] if messages else _GENERIC_ACTIVITY_FAILURE
    if activity_type:
        return f"{activity_type}: {reason}"
    return reason


def _asset_line(state: AssetState) -> str:
    spec = require_asset(state.asset)
    start, end = window_bounds(state.dates)
    count = len(set(state.dates))
    if not start:
        return f"{spec.display}: none (0 sessions)"
    return f"{spec.display}: {start}..{end} ({count:,} sessions)"


def _pair(stocks: int, etfs: int) -> str:
    return f"Stocks {stocks:,}, ETFs {etfs:,}"


def build_idle_message(result: WorkflowResult) -> str:
    lines = [
        "📈 **MarketParquet stocks and ETFs** found nothing to do",
        _pair_window(result),
        "",
        f"Run: `{result.workflow_run_id}`",
    ]
    return _clip("\n".join(lines))


def build_start_message(plan: StockPlan, workflow_run_id: str) -> str:
    stock_download = sum(
        len(chunk) for chunk in fetch_batches(plan.stocks.dates, plan.stocks.csv_dates)
    )
    etf_download = sum(
        len(chunk) for chunk in fetch_batches(plan.etfs.dates, plan.etfs.csv_dates)
    )
    stock_append = sum(
        len(chunk)
        for chunk in append_batches(plan.stocks.dates, plan.stocks.iceberg_dates)
    )
    etf_append = sum(
        len(chunk)
        for chunk in append_batches(plan.etfs.dates, plan.etfs.iceberg_dates)
    )
    lines = [
        "📈 **MarketParquet stocks and ETFs** started",
        _asset_line(plan.stocks),
        _asset_line(plan.etfs),
        f"To download: {_pair(stock_download, etf_download)}",
        f"To append: {_pair(stock_append, etf_append)}",
        "",
        f"Run: `{workflow_run_id}`",
    ]
    return _clip("\n".join(lines))


def build_done_message(result: WorkflowResult) -> str:
    lines = [
        "📈 **MarketParquet stocks and ETFs** finished",
        _pair_window(result),
        (
            "Downloaded: "
            + _pair(result.stock_downloaded, result.etf_downloaded)
        ),
        (
            "Reused Garage CSV: "
            + _pair(
                result.stock_sessions - result.stock_downloaded,
                result.etf_sessions - result.etf_downloaded,
            )
        ),
        (
            "Appended: "
            + _pair(result.stock_appended_days, result.etf_appended_days)
            + f" ({result.appended_rows:,} rows)"
        ),
        (
            "Already stored: "
            + _pair(
                result.stock_sessions - result.stock_appended_days,
                result.etf_sessions - result.etf_appended_days,
            )
        ),
        "",
        f"Run: `{result.workflow_run_id}`",
    ]
    return _clip("\n".join(lines))


def _pair_window(result: WorkflowResult) -> str:
    if not result.window_start:
        return "Window: none (0 sessions)"
    return (
        f"Window: {result.window_start}..{result.window_end} "
        f"(Stocks {result.stock_sessions:,}, ETFs {result.etf_sessions:,})"
    )


def build_failure_message(workflow_run_id: str, reason: str) -> str:
    lines = [
        "⚠️ **MarketParquet stocks and ETFs failed**",
        f"Run: `{workflow_run_id}`",
    ]
    prefix = "\n".join(lines) + "\nError: "
    room = DISCORD_CONTENT_LIMIT - len(prefix)
    cleaned = " ".join(reason.split())
    if room < 1:
        return _clip(prefix)
    if len(cleaned) > room:
        cleaned = cleaned[: room - 1] + "…"
    return prefix + cleaned


def send_discord_message(content: str, settings: Settings | None = None) -> int:
    settings = settings or Settings.from_env()
    timeout = float(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
    response = requests.post(
        settings.discord_webhook_url,
        params={"wait": "true"},
        json={"content": content, "allowed_mentions": {"parse": []}},
        headers={"User-Agent": "finance-tools-marketparquet-etl/0.1"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.status_code
