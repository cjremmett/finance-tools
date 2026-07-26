import asyncio

from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from finance_tools.worker import ACTIVITIES, WORKFLOWS
from interest_rates_scraper.activities import (
    mark_notification_sent_activity,
    persist_interest_rates_activity,
    scrape_fed_rate_activity,
    scrape_kalshi_apy_activity,
    scrape_marcus_cds_activity,
    scrape_marcus_savings_activity,
    send_discord_notification_activity,
)
from interest_rates_scraper.workflows import SCRAPE_ACTIVITIES
from interest_rates_scraper.workflows import InterestRateScrapeWorkflow
from kalshi_markets.activities import (
    mark_kalshi_notification_batch_sent_activity,
    poll_and_stage_kalshi_markets_activity,
    send_kalshi_discord_message_activity,
)
from kalshi_markets.workflows import KalshiMarketMonitorWorkflow


def test_workflow_registers_all_sources() -> None:
    assert SCRAPE_ACTIVITIES == {
        "federal_reserve": "scrape_fed_rate",
        "kalshi": "scrape_kalshi_apy",
        "marcus_savings": "scrape_marcus_savings",
        "marcus_cds": "scrape_marcus_cds",
    }


def test_workflow_passes_temporal_sandbox_validation() -> None:
    async def validate() -> None:
        SandboxedWorkflowRunner().prepare_workflow(
            workflow._Definition.must_from_class(InterestRateScrapeWorkflow)
        )

    asyncio.run(validate())


def test_combined_worker_registers_both_jobs() -> None:
    assert WORKFLOWS == (
        InterestRateScrapeWorkflow,
        KalshiMarketMonitorWorkflow,
    )
    assert ACTIVITIES == (
        scrape_fed_rate_activity,
        scrape_kalshi_apy_activity,
        scrape_marcus_savings_activity,
        scrape_marcus_cds_activity,
        persist_interest_rates_activity,
        send_discord_notification_activity,
        mark_notification_sent_activity,
        poll_and_stage_kalshi_markets_activity,
        send_kalshi_discord_message_activity,
        mark_kalshi_notification_batch_sent_activity,
    )
