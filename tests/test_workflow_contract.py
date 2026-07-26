import asyncio

from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from interest_rates_scraper.workflows import SCRAPE_ACTIVITIES
from interest_rates_scraper.workflows import InterestRateScrapeWorkflow


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
