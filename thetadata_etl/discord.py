from __future__ import annotations

import os

import requests
from temporalio.exceptions import ActivityError

from thetadata_etl.config import Settings
from thetadata_etl.models import (
    BackfillResult,
    BackfillState,
    DatasetResult,
    OptionWorkflowResult,
    WorkflowResult,
)

DISCORD_CONTENT_LIMIT = 2000
_GENERIC_ACTIVITY_FAILURE = "Activity task failed"
_DATASET_LABELS = {
    "stocks": "Stocks",
    "option_underlyings": "Option underlyings",
}


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


def _dataset_line(result: DatasetResult) -> str:
    source = (
        "fetched from ThetaData" if result.fetched else "reused Garage CSV"
    )
    stored = "appended" if result.appended else "already stored"
    label = _DATASET_LABELS.get(result.dataset, result.dataset)
    return (
        f"• **{label}**: {result.row_count:,} unique symbols ({source}, {stored})"
    )


def build_securities_message(result: WorkflowResult) -> str:
    lines = [
        f"📈 **ThetaData securities** — {result.fetch_date}",
        *[_dataset_line(dataset) for dataset in result.datasets],
        "",
        f"Run: `{result.workflow_run_id}`",
    ]
    return _clip("\n".join(lines))


def build_option_message(result: OptionWorkflowResult) -> str:
    if result.skipped:
        lines = [
            "📈 **ThetaData option sync** skipped",
            result.skip_reason,
        ]
    elif not result.symbol_snapshot_date:
        lines = [
            f"📈 **ThetaData option sync** — {result.trade_date}",
            (
                f"Session already stored ({result.row_count:,} rows). "
                "ThetaData was not called."
            ),
        ]
    else:
        lines = [
            f"📈 **ThetaData option sync** — {result.trade_date}",
            f"Snapshot: {result.symbol_snapshot_date}",
            f"Underlyings: {result.underlying_count:,}",
            f"Symbols checkpointed: {result.fetched_count:,}",
            f"Rows appended: {result.appended_rows:,}",
            f"Stored rows: {result.row_count:,}",
        ]
    lines.extend(["", f"Run: `{result.workflow_run_id}`"])
    return _clip("\n".join(lines))


def build_backfill_started_message(
    state: BackfillState, workflow_run_id: str
) -> str:
    lines = [
        "📈 **ThetaData option backfill** started",
        (
            f"Window: {state.window_start}..{state.window_end} "
            f"({len(state.sessions):,} sessions)"
        ),
        f"Excluded latest session: {state.excluded}",
        f"Symbols to backfill: {len(state.symbols):,}",
        f"Already covered: {state.covered:,}",
        f"Snapshot: {state.symbol_snapshot_date}",
        "",
        f"Run: `{workflow_run_id}`",
    ]
    return _clip("\n".join(lines))


def build_backfill_idle_message(result: BackfillResult) -> str:
    lines = [
        "📈 **ThetaData option backfill** found nothing to do",
        f"Window: {result.window_start}..{result.window_end}",
        f"Excluded latest session: {result.excluded}",
        f"Symbols already covered: {result.covered:,}",
        f"Snapshot: {result.symbol_snapshot_date}",
        "",
        f"Run: `{result.workflow_run_id}`",
    ]
    return _clip("\n".join(lines))


def build_backfill_done_message(result: BackfillResult) -> str:
    lines = [
        "📈 **ThetaData option backfill** finished",
        f"Window: {result.window_start}..{result.window_end}",
        f"Excluded latest session: {result.excluded}",
        f"Symbols completed: {result.completed:,}",
        f"Symbols failed: {len(result.failed):,}",
        f"Sessions fetched: {result.fetched_sessions:,}",
        f"Rows appended: {result.appended_rows:,}",
        f"Already covered: {result.covered:,}",
        "",
        f"Run: `{result.workflow_run_id}`",
    ]
    return _with_failed_symbols(lines, result.failed)


def build_failure_message(
    job_name: str,
    workflow_run_id: str,
    reason: str,
    *,
    given_up: int | None = None,
) -> str:
    lines = [f"⚠️ **{job_name} failed**"]
    if given_up is not None:
        lines.append(f"Symbols already given up: {given_up:,}")
    lines.append(f"Run: `{workflow_run_id}`")
    prefix = "\n".join(lines) + "\nError: "
    room = DISCORD_CONTENT_LIMIT - len(prefix)
    cleaned = " ".join(reason.split())
    if room < 1:
        return _clip(prefix)
    if len(cleaned) > room:
        cleaned = cleaned[: room - 1] + "…"
    return prefix + cleaned


def _with_failed_symbols(lines: list[str], symbols: list[str]) -> str:
    header = "\n".join(lines)
    if not symbols:
        return _clip(header)
    shown: list[str] = []
    for index, symbol in enumerate(symbols):
        remaining_after = len(symbols) - index - 1
        suffix = f" and {remaining_after:,} more" if remaining_after else ""
        candidate = header + "\nFailed: " + ", ".join([*shown, symbol]) + suffix
        if len(candidate) > DISCORD_CONTENT_LIMIT:
            left = len(symbols) - index
            if shown:
                text = (
                    header
                    + "\nFailed: "
                    + ", ".join(shown)
                    + f" and {left:,} more"
                )
            else:
                text = header + f"\nFailed: {left:,} symbols"
            return _clip(text)
        shown.append(symbol)
    return header + "\nFailed: " + ", ".join(shown)


def send_discord_message(content: str, settings: Settings | None = None) -> int:
    settings = settings or Settings.from_env()
    timeout = float(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
    response = requests.post(
        settings.discord_webhook_url,
        params={"wait": "true"},
        json={"content": content, "allowed_mentions": {"parse": []}},
        headers={"User-Agent": "finance-tools-thetadata-etl/0.1"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.status_code
