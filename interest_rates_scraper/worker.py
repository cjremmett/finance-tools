from __future__ import annotations

import asyncio
import logging
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
from interest_rates_scraper.config import Settings
from interest_rates_scraper.workflows import InterestRateScrapeWorkflow


async def run_worker() -> None:
    settings = Settings.from_env()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    with ThreadPoolExecutor(
        max_workers=8, thread_name_prefix="temporal-activity"
    ) as activity_executor:
        worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=[InterestRateScrapeWorkflow],
            activities=[
                scrape_fed_rate_activity,
                scrape_kalshi_apy_activity,
                scrape_marcus_savings_activity,
                scrape_marcus_cds_activity,
                persist_interest_rates_activity,
                send_discord_notification_activity,
                mark_notification_sent_activity,
            ],
            activity_executor=activity_executor,
        )
        logging.info(
            "Polling Temporal at %s/%s on task queue %s",
            settings.temporal_address,
            settings.temporal_namespace,
            settings.temporal_task_queue,
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
