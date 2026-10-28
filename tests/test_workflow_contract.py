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
from thetadata_etl.activities import (
    append_option_backfill_batch_activity,
    append_option_batch_activity,
    append_symbols_to_iceberg_activity,
    complete_option_day_activity,
    ensure_securities_catalog_activity,
    fetch_and_upload_symbols_activity,
    fetch_option_backfill_symbol_activity,
    fetch_option_batch_activity,
    option_day_loaded_activity,
    confirm_option_open_interest_activity,
    plan_option_backfill_activity,
    plan_option_batches_activity,
    resolve_option_session_activity,
    resolve_symbol_fetch_date_activity,
    symbol_csv_exists_activity,
)
from thetadata_etl import worker as theta_worker
from thetadata_etl.workflows import (
    ThetaDataOptionBackfillWorkflow,
    ThetaDataOptionDataWorkflow,
    ThetaDataSecuritiesWorkflow,
)


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
        ThetaDataSecuritiesWorkflow,
        ThetaDataOptionDataWorkflow,
        ThetaDataOptionBackfillWorkflow,
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
        resolve_symbol_fetch_date_activity,
        ensure_securities_catalog_activity,
        symbol_csv_exists_activity,
        fetch_and_upload_symbols_activity,
        append_symbols_to_iceberg_activity,
        resolve_option_session_activity,
        option_day_loaded_activity,
        confirm_option_open_interest_activity,
        plan_option_batches_activity,
        fetch_option_batch_activity,
        append_option_batch_activity,
        complete_option_day_activity,
        plan_option_backfill_activity,
        fetch_option_backfill_symbol_activity,
        append_option_backfill_batch_activity,
    )


def test_thetadata_worker_registers_the_backfill() -> None:
    assert theta_worker.WORKFLOWS == (
        ThetaDataSecuritiesWorkflow,
        ThetaDataOptionDataWorkflow,
        ThetaDataOptionBackfillWorkflow,
    )
    assert theta_worker.ACTIVITIES[-3:] == (
        plan_option_backfill_activity,
        fetch_option_backfill_symbol_activity,
        append_option_backfill_batch_activity,
    )
    assert len(theta_worker.ACTIVITIES) == 15
