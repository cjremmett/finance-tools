from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from thetadata_etl.datasets import (
        APPEND_SYMBOLS_TO_ICEBERG,
        DATASETS,
        ENSURE_SECURITIES_CATALOG,
        FETCH_AND_UPLOAD_SYMBOLS,
        RESOLVE_SYMBOL_FETCH_DATE,
        SYMBOL_CSV_EXISTS,
    )
    from thetadata_etl.models import (
        AppendResult,
        DatasetResult,
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
