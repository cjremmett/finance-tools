from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from marketparquet_etl.discord import (
        build_done_message,
        build_failure_message,
        build_idle_message,
        build_start_message,
        failure_reason,
    )
    from marketparquet_etl.models import (
        APPEND_MARKETPARQUET_STOCK_DAYS,
        ENSURE_MARKETPARQUET_CATALOG,
        ETFS,
        FETCH_MARKETPARQUET_STOCK_DAYS,
        PLAN_MARKETPARQUET_STOCK_SYNC,
        SEND_MARKETPARQUET_DISCORD,
        STOCKS,
        WORKFLOW_NAME,
        AppendChunkResult,
        AssetChunk,
        FetchChunkResult,
        StockPlan,
        WorkflowResult,
        append_batches,
        fetch_batches,
        window_bounds,
    )


@workflow.defn(name=WORKFLOW_NAME)
class MarketParquetStockDataWorkflow:
    @workflow.run
    async def run(self) -> WorkflowResult:
        info = workflow.info()
        retry_policy = _retry_policy()
        try:
            return await _run(info.run_id, retry_policy)
        except Exception as error:
            if is_cancelled_exception(error):
                raise
            await _report_failure(info.run_id, error, retry_policy)
            raise


async def _run(workflow_run_id: str, retry_policy: RetryPolicy) -> WorkflowResult:
    await workflow.execute_activity(
        ENSURE_MARKETPARQUET_CATALOG,
        start_to_close_timeout=timedelta(minutes=2),
        retry_policy=retry_policy,
    )
    plan: StockPlan = await workflow.execute_activity(
        PLAN_MARKETPARQUET_STOCK_SYNC,
        start_to_close_timeout=timedelta(minutes=5),
        heartbeat_timeout=timedelta(seconds=60),
        retry_policy=retry_policy,
        result_type=StockPlan,
    )
    downloads = _chunks(plan, "fetch")
    appends = _chunks(plan, "append")
    counts = _session_counts(plan)
    start, end = window_bounds(plan.stocks.dates + plan.etfs.dates)
    if not downloads and not appends:
        result = _result(
            workflow_run_id,
            start,
            end,
            counts,
            downloaded={STOCKS: 0, ETFS: 0},
            appended_days={STOCKS: 0, ETFS: 0},
            appended_rows={STOCKS: 0, ETFS: 0},
            idle=True,
        )
        await _send(build_idle_message(result), retry_policy)
        return result

    await _send(build_start_message(plan, workflow_run_id), retry_policy)
    downloaded = {STOCKS: 0, ETFS: 0}
    for chunk in downloads:
        fetched: FetchChunkResult = await workflow.execute_activity(
            FETCH_MARKETPARQUET_STOCK_DAYS,
            chunk,
            start_to_close_timeout=timedelta(minutes=20),
            schedule_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
            result_type=FetchChunkResult,
        )
        downloaded[chunk.asset] += len(fetched.dates)
    appended_days = {STOCKS: 0, ETFS: 0}
    appended_rows = {STOCKS: 0, ETFS: 0}
    for chunk in appends:
        appended: AppendChunkResult = await workflow.execute_activity(
            APPEND_MARKETPARQUET_STOCK_DAYS,
            chunk,
            start_to_close_timeout=timedelta(minutes=20),
            schedule_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
            result_type=AppendChunkResult,
        )
        appended_days[chunk.asset] += appended.appended_days
        appended_rows[chunk.asset] += appended.appended_rows
    result = _result(
        workflow_run_id,
        start,
        end,
        counts,
        downloaded,
        appended_days,
        appended_rows,
        idle=False,
    )
    await _send(build_done_message(result), retry_policy)
    return result


def _chunks(plan: StockPlan, kind: str) -> list[AssetChunk]:
    chunks: list[AssetChunk] = []
    for state in (plan.stocks, plan.etfs):
        if kind == "fetch":
            groups = fetch_batches(state.dates, state.csv_dates)
        else:
            groups = append_batches(state.dates, state.iceberg_dates)
        for dates in groups:
            chunks.append(AssetChunk(asset=state.asset, dates=dates))
    return chunks


def _session_counts(plan: StockPlan) -> dict[str, int]:
    return {
        STOCKS: len(set(plan.stocks.dates)),
        ETFS: len(set(plan.etfs.dates)),
    }


def _result(
    workflow_run_id: str,
    start: str,
    end: str,
    counts: dict[str, int],
    downloaded: dict[str, int],
    appended_days: dict[str, int],
    appended_rows: dict[str, int],
    idle: bool,
) -> WorkflowResult:
    session_count = counts[STOCKS] + counts[ETFS]
    downloaded_total = downloaded[STOCKS] + downloaded[ETFS]
    appended_total = appended_days[STOCKS] + appended_days[ETFS]
    return WorkflowResult(
        workflow_run_id=workflow_run_id,
        window_start=start,
        window_end=end,
        session_count=session_count,
        downloaded=downloaded_total,
        reused=session_count - downloaded_total,
        appended_days=appended_total,
        appended_rows=appended_rows[STOCKS] + appended_rows[ETFS],
        already_stored=session_count - appended_total,
        idle=idle,
        stock_sessions=counts[STOCKS],
        etf_sessions=counts[ETFS],
        stock_downloaded=downloaded[STOCKS],
        etf_downloaded=downloaded[ETFS],
        stock_appended_days=appended_days[STOCKS],
        etf_appended_days=appended_days[ETFS],
        stock_appended_rows=appended_rows[STOCKS],
        etf_appended_rows=appended_rows[ETFS],
    )


def _retry_policy() -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=5),
        backoff_coefficient=2,
        maximum_interval=timedelta(minutes=1),
        maximum_attempts=3,
    )


async def _send(content: str, retry_policy: RetryPolicy) -> None:
    await workflow.execute_activity(
        SEND_MARKETPARQUET_DISCORD,
        content,
        start_to_close_timeout=timedelta(seconds=30),
        retry_policy=retry_policy,
    )


async def _report_failure(
    workflow_run_id: str, error: BaseException, retry_policy: RetryPolicy
) -> None:
    await _send(
        build_failure_message(workflow_run_id, failure_reason(error)),
        retry_policy,
    )
