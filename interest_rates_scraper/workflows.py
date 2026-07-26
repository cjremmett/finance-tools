from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from interest_rates_scraper.models import (
    DiscordNotification,
    PersistRequest,
    PersistResult,
    ScrapeResult,
    SourceFailure,
    WorkflowResult,
)

SCRAPE_ACTIVITIES = {
    "federal_reserve": "scrape_fed_rate",
    "kalshi": "scrape_kalshi_apy",
    "marcus_savings": "scrape_marcus_savings",
    "marcus_cds": "scrape_marcus_cds",
}


@workflow.defn(name="InterestRateScrapeWorkflow")
class InterestRateScrapeWorkflow:
    @workflow.run
    async def run(self) -> WorkflowResult:
        info = workflow.info()
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2,
            maximum_interval=timedelta(minutes=1),
            maximum_attempts=3,
        )
        tasks = {
            source: workflow.execute_activity(
                activity_name,
                start_to_close_timeout=timedelta(seconds=45),
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry_policy,
                result_type=ScrapeResult,
            )
            for source, activity_name in SCRAPE_ACTIVITIES.items()
        }

        outcomes = await asyncio.gather(*tasks.values(), return_exceptions=True)
        results: list[ScrapeResult] = []
        failures: list[SourceFailure] = []
        for source, outcome in zip(tasks.keys(), outcomes, strict=True):
            if isinstance(outcome, BaseException):
                failures.append(SourceFailure(source=source, error=str(outcome)))
            else:
                results.append(outcome)

        observed_at = workflow.now().isoformat()
        persisted: PersistResult = await workflow.execute_activity(
            "persist_interest_rates",
            PersistRequest(
                workflow_id=info.workflow_id,
                workflow_run_id=info.run_id,
                observed_at=observed_at,
                results=results,
                failures=failures,
            ),
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=retry_policy,
            result_type=PersistResult,
        )

        notification_sent = False
        if persisted.changes or failures:
            await workflow.execute_activity(
                "send_discord_notification",
                DiscordNotification(
                    workflow_run_id=info.run_id,
                    changes=persisted.changes,
                    failures=failures,
                    baseline=persisted.baseline,
                ),
                start_to_close_timeout=timedelta(seconds=45),
                retry_policy=retry_policy,
            )
            await workflow.execute_activity(
                "mark_notification_sent",
                info.run_id,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=retry_policy,
            )
            notification_sent = True

        return WorkflowResult(
            workflow_run_id=info.run_id,
            successful_sources=sorted(result.source for result in results),
            failed_sources=sorted(failure.source for failure in failures),
            changes=len(persisted.changes),
            notification_sent=notification_sent,
        )
