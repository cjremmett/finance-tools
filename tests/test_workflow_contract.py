import asyncio

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from crypto_carry.activities import (
    fetch_crypto_carry_activity,
    send_crypto_carry_discord_message_activity,
)
from crypto_carry.workflows import CryptoCarryWorkflow
from finance_tools.worker import ACTIVITIES, WORKFLOWS
from interest_rates_scraper.activities import (
    mark_notification_sent_activity,
    persist_interest_rates_activity,
    scrape_fed_rate_activity,
    scrape_kalshi_apy_activity,
    scrape_marcus_cds_activity,
    scrape_marcus_savings_activity,
    scrape_x_money_apy_activity,
    send_discord_notification_activity,
)
from interest_rates_scraper.models import FED_MAINTENANCE_ERROR
from interest_rates_scraper.workflows import (
    SCRAPE_ACTIVITIES,
    InterestRateScrapeWorkflow,
    failure_message,
)
from kalshi_markets.activities import (
    mark_kalshi_notification_batch_sent_activity,
    poll_and_stage_kalshi_markets_activity,
    send_kalshi_discord_message_activity,
)
from kalshi_markets.workflows import KalshiMarketMonitorWorkflow


def _activity_error(message: str, cause: BaseException | None = None) -> ActivityError:
    error = ActivityError(
        "Activity task failed",
        scheduled_event_id=1,
        started_event_id=2,
        identity="worker",
        activity_type="scrape_fed_rate",
        activity_id="1",
        retry_state=None,
    )
    error.__cause__ = cause if cause is not None else ApplicationError(
        message, type="ScrapeError"
    )
    return error


def test_failure_message_uses_fed_maintenance_cause() -> None:
    error = _activity_error(FED_MAINTENANCE_ERROR)
    assert failure_message(error) == FED_MAINTENANCE_ERROR
    assert "Activity task failed" not in failure_message(error)


def test_failure_message_leaves_other_activity_errors_unchanged() -> None:
    error = _activity_error("FRED API request failed for series DFEDTARL (JSONDecodeError)")
    assert failure_message(error) == "Activity task failed"


def test_workflow_registers_all_sources() -> None:
    assert SCRAPE_ACTIVITIES == {
        "federal_reserve": "scrape_fed_rate",
        "kalshi": "scrape_kalshi_apy",
        "marcus_savings": "scrape_marcus_savings",
        "marcus_cds": "scrape_marcus_cds",
        "x_money": "scrape_x_money_apy",
    }


def test_workflow_passes_temporal_sandbox_validation() -> None:
    async def validate() -> None:
        SandboxedWorkflowRunner().prepare_workflow(
            workflow._Definition.must_from_class(InterestRateScrapeWorkflow)
        )

    asyncio.run(validate())


def test_combined_worker_registers_all_jobs() -> None:
    assert WORKFLOWS == (
        InterestRateScrapeWorkflow,
        KalshiMarketMonitorWorkflow,
        CryptoCarryWorkflow,
    )
    assert ACTIVITIES == (
        scrape_fed_rate_activity,
        scrape_kalshi_apy_activity,
        scrape_marcus_savings_activity,
        scrape_marcus_cds_activity,
        scrape_x_money_apy_activity,
        persist_interest_rates_activity,
        send_discord_notification_activity,
        mark_notification_sent_activity,
        poll_and_stage_kalshi_markets_activity,
        send_kalshi_discord_message_activity,
        mark_kalshi_notification_batch_sent_activity,
        fetch_crypto_carry_activity,
        send_crypto_carry_discord_message_activity,
    )
