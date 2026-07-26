from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from interest_rates_scraper.activities import (
    mark_notification_sent_activity,
    persist_interest_rates_activity,
    scrape_fed_rate_activity,
    scrape_kalshi_apy_activity,
    scrape_marcus_cds_activity,
    scrape_marcus_savings_activity,
    send_discord_notification_activity,
)
from interest_rates_scraper.workflows import InterestRateScrapeWorkflow
from kalshi_markets.activities import (
    mark_kalshi_notification_batch_sent_activity,
    poll_and_stage_kalshi_markets_activity,
    send_kalshi_discord_message_activity,
)
from kalshi_markets.workflows import KalshiMarketMonitorWorkflow

WORKFLOWS = (
    InterestRateScrapeWorkflow,
    KalshiMarketMonitorWorkflow,
)

ACTIVITIES = (
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


async def run_worker() -> None:
    temporal_address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    temporal_namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    temporal_task_queue = os.getenv("TEMPORAL_TASK_QUEUE", "finance-tools")
    client = await Client.connect(
        temporal_address,
        namespace=temporal_namespace,
    )
    with ThreadPoolExecutor(
        max_workers=12,
        thread_name_prefix="finance-tools-activity",
    ) as activity_executor:
        worker = Worker(
            client,
            task_queue=temporal_task_queue,
            workflows=list(WORKFLOWS),
            activities=list(ACTIVITIES),
            activity_executor=activity_executor,
        )
        logging.info(
            "Polling Temporal at %s/%s on shared task queue %s for workflows %s",
            temporal_address,
            temporal_namespace,
            temporal_task_queue,
            ", ".join(workflow.__name__ for workflow in WORKFLOWS),
        )
        await worker.run()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()

