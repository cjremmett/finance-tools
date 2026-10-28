from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from thetadata_etl.datasets import (
        APPEND_OPTION_BACKFILL_BATCH,
        APPEND_OPTION_BATCH,
        APPEND_SYMBOLS_TO_ICEBERG,
        BACKFILL_SYMBOL_FAILED,
        COMPLETE_OPTION_DAY,
        CONFIRM_OPTION_OPEN_INTEREST,
        DATASETS,
        ENSURE_SECURITIES_CATALOG,
        FETCH_AND_UPLOAD_SYMBOLS,
        FETCH_OPTION_BACKFILL_SYMBOL,
        FETCH_OPTION_BATCH,
        OPTION_BACKFILL_WORKFLOW,
        OPTION_DATA_WORKFLOW,
        OPTION_DAY_LOADED,
        PLAN_OPTION_BACKFILL,
        PLAN_OPTION_BATCHES,
        RESOLVE_OPTION_SESSION,
        RESOLVE_SYMBOL_FETCH_DATE,
        SYMBOL_CSV_EXISTS,
    )
    from thetadata_etl.models import (
        AppendResult,
        BackfillAppendRequest,
        BackfillPlan,
        BackfillResult,
        BackfillState,
        BackfillSymbolRequest,
        BackfillSymbolResult,
        BatchAppendResult,
        BatchFetchResult,
        DatasetResult,
        LoadedDay,
        OptionBatch,
        OptionPlan,
        OptionWorkflowResult,
        SessionResolution,
        SymbolSnapshotRequest,
        WorkflowResult,
    )


@workflow.defn(name="ThetaDataSecuritiesWorkflow")
class ThetaDataSecuritiesWorkflow:
    @workflow.run
    async def run(self) -> WorkflowResult:
        info = workflow.info()
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2,
            maximum_interval=timedelta(minutes=1),
            maximum_attempts=3,
        )
        fetch_date = await workflow.execute_activity(
            RESOLVE_SYMBOL_FETCH_DATE,
            workflow.now().isoformat(),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=retry_policy,
            result_type=str,
        )
        await workflow.execute_activity(
            ENSURE_SECURITIES_CATALOG,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=retry_policy,
        )
        datasets: list[DatasetResult] = []
        for dataset in DATASETS:
            request = SymbolSnapshotRequest(dataset=dataset, fetch_date=fetch_date)
            exists = await workflow.execute_activity(
                SYMBOL_CSV_EXISTS,
                request,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=retry_policy,
                result_type=bool,
            )
            fetched = False
            if not exists:
                await workflow.execute_activity(
                    FETCH_AND_UPLOAD_SYMBOLS,
                    request,
                    start_to_close_timeout=timedelta(minutes=5),
                    schedule_to_close_timeout=timedelta(minutes=15),
                    retry_policy=retry_policy,
                    result_type=int,
                )
                fetched = True
            appended: AppendResult = await workflow.execute_activity(
                APPEND_SYMBOLS_TO_ICEBERG,
                request,
                start_to_close_timeout=timedelta(minutes=5),
                schedule_to_close_timeout=timedelta(minutes=15),
                retry_policy=retry_policy,
                result_type=AppendResult,
            )
            datasets.append(
                DatasetResult(
                    dataset=dataset,
                    fetched=fetched,
                    appended=appended.appended,
                    row_count=appended.row_count,
                )
            )
        return WorkflowResult(
            workflow_run_id=info.run_id,
            fetch_date=fetch_date,
            datasets=datasets,
        )


def _retry_policy() -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=5),
        backoff_coefficient=2,
        maximum_interval=timedelta(minutes=1),
        maximum_attempts=3,
    )


def _open_interest_retry_policy() -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(minutes=5),
        backoff_coefficient=1,
        maximum_interval=timedelta(minutes=5),
        maximum_attempts=6,
    )


@workflow.defn(name=OPTION_DATA_WORKFLOW)
class ThetaDataOptionDataWorkflow:
    @workflow.run
    async def run(self) -> OptionWorkflowResult:
        info = workflow.info()
        retry_policy = _retry_policy()
        session: SessionResolution = await workflow.execute_activity(
            RESOLVE_OPTION_SESSION,
            workflow.now().isoformat(),
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
            result_type=SessionResolution,
        )
        if session.skipped:
            return _option_result(
                info.run_id,
                trade_date="",
                skipped=True,
                skip_reason=session.skip_reason,
            )
        loaded: LoadedDay = await workflow.execute_activity(
            OPTION_DAY_LOADED,
            session.trade_date,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=retry_policy,
            result_type=LoadedDay,
        )
        if loaded.loaded:
            return _option_result(
                info.run_id,
                trade_date=session.trade_date,
                row_count=loaded.row_count,
            )
        await workflow.execute_activity(
            CONFIRM_OPTION_OPEN_INTEREST,
            session,
            start_to_close_timeout=timedelta(minutes=10),
            schedule_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(seconds=60),
            retry_policy=_open_interest_retry_policy(),
        )
        plan: OptionPlan = await workflow.execute_activity(
            PLAN_OPTION_BATCHES,
            session.trade_date,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=retry_policy,
            result_type=OptionPlan,
        )
        fetched_count = 0
        appended_rows = 0
        for batch_index, symbols in enumerate(plan.batches):
            batch = OptionBatch(
                trade_date=session.trade_date,
                oi_end_date=session.oi_end_date,
                batch_index=batch_index,
                symbols=symbols,
            )
            fetched: BatchFetchResult = await workflow.execute_activity(
                FETCH_OPTION_BATCH,
                batch,
                start_to_close_timeout=timedelta(minutes=15),
                schedule_to_close_timeout=timedelta(minutes=45),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry_policy,
                result_type=BatchFetchResult,
            )
            fetched_count += fetched.fetched
            appended: BatchAppendResult = await workflow.execute_activity(
                APPEND_OPTION_BATCH,
                batch,
                start_to_close_timeout=timedelta(minutes=5),
                schedule_to_close_timeout=timedelta(minutes=15),
                retry_policy=retry_policy,
                result_type=BatchAppendResult,
            )
            appended_rows += appended.appended_rows
        row_count = await workflow.execute_activity(
            COMPLETE_OPTION_DAY,
            session.trade_date,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=retry_policy,
            result_type=int,
        )
        return _option_result(
            info.run_id,
            trade_date=session.trade_date,
            symbol_snapshot_date=plan.symbol_snapshot_date,
            underlying_count=plan.underlying_count,
            fetched_count=fetched_count,
            appended_rows=appended_rows,
            row_count=row_count,
        )


def _backfill_retry_policy() -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=5),
        backoff_coefficient=2,
        maximum_interval=timedelta(minutes=1),
        maximum_attempts=0,
        non_retryable_error_types=[BACKFILL_SYMBOL_FAILED],
    )


def _gave_up(error: BaseException) -> bool:
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ApplicationError) and current.type == BACKFILL_SYMBOL_FAILED:
            return True
        cause = current.__cause__
        if cause is None:
            cause = getattr(current, "cause", None)
        if not isinstance(cause, BaseException) or cause is current:
            cause = current.__context__ if isinstance(current.__context__, BaseException) else None
        current = cause
    return False


@workflow.defn(name=OPTION_BACKFILL_WORKFLOW)
class ThetaDataOptionBackfillWorkflow:
    @workflow.run
    async def run(self, state: BackfillState | None = None) -> BackfillResult:
        info = workflow.info()
        retry_policy = _backfill_retry_policy()
        if state is None:
            plan: BackfillPlan = await workflow.execute_activity(
                PLAN_OPTION_BACKFILL,
                workflow.now().isoformat(),
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry_policy,
                result_type=BackfillPlan,
            )
            state = BackfillState(
                window_start=plan.window_start,
                window_end=plan.window_end,
                excluded=plan.excluded,
                sessions=plan.sessions,
                symbols=plan.symbols,
                next_index=0,
                batch_size=plan.batch_size,
                symbol_snapshot_date=plan.symbol_snapshot_date,
                fetched_sessions=0,
                appended_rows=0,
                failed=[],
                covered=plan.covered,
            )
        if not state.symbols or state.next_index >= len(state.symbols):
            return _backfill_result(info.run_id, state)
        batch = state.symbols[state.next_index : state.next_index + state.batch_size]
        completed: list[str] = []
        for symbol in batch:
            request = BackfillSymbolRequest(
                symbol=symbol,
                sessions=state.sessions,
                excluded=state.excluded,
                window_start=state.window_start,
                window_end=state.window_end,
            )
            try:
                fetched: BackfillSymbolResult = await workflow.execute_activity(
                    FETCH_OPTION_BACKFILL_SYMBOL,
                    request,
                    start_to_close_timeout=timedelta(hours=24),
                    heartbeat_timeout=timedelta(seconds=60),
                    retry_policy=retry_policy,
                    result_type=BackfillSymbolResult,
                )
            except ActivityError as error:
                if not _gave_up(error):
                    raise
                state.failed.append(symbol)
                continue
            completed.append(symbol)
            state.fetched_sessions += fetched.fetched_sessions
        if completed:
            appended = await workflow.execute_activity(
                APPEND_OPTION_BACKFILL_BATCH,
                BackfillAppendRequest(
                    symbols=completed,
                    sessions=state.sessions,
                    window_start=state.window_start,
                    window_end=state.window_end,
                    batch_index=state.next_index // max(state.batch_size, 1),
                ),
                start_to_close_timeout=timedelta(hours=2),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry_policy,
                result_type=int,
            )
            state.appended_rows += appended
        state.next_index += len(batch)
        if state.next_index < len(state.symbols):
            workflow.continue_as_new(state)
        return _backfill_result(info.run_id, state)


def _backfill_result(workflow_run_id: str, state: BackfillState) -> BackfillResult:
    return BackfillResult(
        workflow_run_id=workflow_run_id,
        window_start=state.window_start,
        window_end=state.window_end,
        excluded=state.excluded,
        symbol_snapshot_date=state.symbol_snapshot_date,
        covered=state.covered,
        completed=state.next_index - len(state.failed),
        failed=list(state.failed),
        fetched_sessions=state.fetched_sessions,
        appended_rows=state.appended_rows,
    )


def _option_result(
    workflow_run_id: str,
    *,
    trade_date: str,
    skipped: bool = False,
    skip_reason: str = "",
    symbol_snapshot_date: str = "",
    underlying_count: int = 0,
    fetched_count: int = 0,
    appended_rows: int = 0,
    row_count: int = 0,
) -> OptionWorkflowResult:
    return OptionWorkflowResult(
        workflow_run_id=workflow_run_id,
        trade_date=trade_date,
        skipped=skipped,
        skip_reason=skip_reason,
        symbol_snapshot_date=symbol_snapshot_date,
        underlying_count=underlying_count,
        fetched_count=fetched_count,
        appended_rows=appended_rows,
        row_count=row_count,
    )
