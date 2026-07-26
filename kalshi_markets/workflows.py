from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from kalshi_markets.discord import build_failure_message
    from kalshi_markets.models import (
        MarkBatchRequest,
        PollRequest,
        StageResult,
        WorkflowResult,
    )


@workflow.defn(name="KalshiMarketMonitorWorkflow")
class KalshiMarketMonitorWorkflow:
    @workflow.run
    async def run(self) -> WorkflowResult:
        info = workflow.info()
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2,
            maximum_interval=timedelta(minutes=1),
            maximum_attempts=3,
        )
        stage_name = "polling and persistence"
        batches_sent = 0
        try:
            for pass_number in range(2):
                staged: StageResult = await workflow.execute_activity(
                    "poll_and_stage_kalshi_markets",
                    PollRequest(
                        workflow_id=info.workflow_id,
                        workflow_run_id=info.run_id,
                        observed_at=workflow.now().isoformat(),
                    ),
                    start_to_close_timeout=timedelta(minutes=10),
                    schedule_to_close_timeout=timedelta(minutes=35),
                    retry_policy=retry_policy,
                    result_type=StageResult,
                )
                stage_name = "Discord notification"
                for index in range(
                    staged.next_batch_index, len(staged.notification_batches)
                ):
                    await workflow.execute_activity(
                        "send_kalshi_discord_message",
                        staged.notification_batches[index],
                        start_to_close_timeout=timedelta(minutes=2),
                        schedule_to_close_timeout=timedelta(minutes=10),
                        retry_policy=retry_policy,
                    )
                    await workflow.execute_activity(
                        "mark_kalshi_notification_batch_sent",
                        MarkBatchRequest(
                            workflow_run_id=staged.staged_workflow_run_id,
                            batch_index=index,
                        ),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=retry_policy,
                    )
                    batches_sent += 1
                    if index + 1 < len(staged.notification_batches):
                        await workflow.sleep(
                            timedelta(
                                seconds=staged.discord_batch_delay_seconds
                            )
                        )
                if not staged.pending_existing:
                    return WorkflowResult(
                        workflow_run_id=info.run_id,
                        new_market_count=staged.new_market_count,
                        new_category_count=staged.new_category_count,
                        notification_batches_sent=batches_sent,
                        initialized=staged.initialized,
                    )
                stage_name = "polling and persistence"
                if pass_number == 1:
                    raise RuntimeError(
                        "pending notifications could not be drained"
                    )
        except Exception as error:
            failure_message = build_failure_message(
                info.run_id, stage_name, str(error)
            )
            await workflow.execute_activity(
                "send_kalshi_discord_message",
                failure_message,
                start_to_close_timeout=timedelta(minutes=2),
                schedule_to_close_timeout=timedelta(minutes=10),
                retry_policy=retry_policy,
            )
            raise
        raise RuntimeError("Kalshi workflow reached an invalid state")
