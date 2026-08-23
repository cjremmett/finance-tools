from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from crypto_carry.discord import build_digest_messages, build_failure_message
    from crypto_carry.models import CarrySnapshot, FetchRequest, WorkflowResult


@workflow.defn(name="CryptoCarryWorkflow")
class CryptoCarryWorkflow:
    @workflow.run
    async def run(self) -> WorkflowResult:
        info = workflow.info()
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2,
            maximum_interval=timedelta(minutes=1),
            maximum_attempts=3,
        )
        try:
            snapshot: CarrySnapshot = await workflow.execute_activity(
                "fetch_crypto_carry",
                FetchRequest(observed_at=workflow.now().isoformat()),
                start_to_close_timeout=timedelta(seconds=45),
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy,
                result_type=CarrySnapshot,
            )
            messages = build_digest_messages(snapshot, info.run_id)
            for content in messages:
                await workflow.execute_activity(
                    "send_crypto_carry_discord_message",
                    content,
                    start_to_close_timeout=timedelta(seconds=45),
                    retry_policy=retry_policy,
                )
            return WorkflowResult(
                workflow_run_id=info.run_id,
                btc_contract_count=sum(
                    1 for row in snapshot.rows if row.asset == "BTC"
                ),
                eth_contract_count=sum(
                    1 for row in snapshot.rows if row.asset == "ETH"
                ),
                notification_batches_sent=len(messages),
            )
        except Exception as error:
            failure_message = build_failure_message(info.run_id, str(error))
            await workflow.execute_activity(
                "send_crypto_carry_discord_message",
                failure_message,
                start_to_close_timeout=timedelta(seconds=45),
                retry_policy=retry_policy,
            )
            raise
