from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from kalshi_markets.activities import (
    mark_kalshi_notification_batch_sent_activity,
    poll_and_stage_kalshi_markets_activity,
    send_kalshi_discord_message_activity,
)
from kalshi_markets.config import Settings
from kalshi_markets.workflows import KalshiMarketMonitorWorkflow


async def run_worker() -> None:
    settings = Settings.from_env()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    with ThreadPoolExecutor(
        max_workers=4, thread_name_prefix="kalshi-market-activity"
    ) as activity_executor:
        worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=[KalshiMarketMonitorWorkflow],
            activities=[
                poll_and_stage_kalshi_markets_activity,
                send_kalshi_discord_message_activity,
                mark_kalshi_notification_batch_sent_activity,
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

