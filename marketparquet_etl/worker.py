from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from marketparquet_etl.activities import (
    append_marketparquet_stock_days_activity,
    ensure_marketparquet_catalog_activity,
    fetch_marketparquet_stock_days_activity,
    plan_marketparquet_stock_sync_activity,
    send_marketparquet_discord_message_activity,
)
from marketparquet_etl.config import Settings
from marketparquet_etl.workflows import MarketParquetStockDataWorkflow

WORKFLOWS = (MarketParquetStockDataWorkflow,)

ACTIVITIES = (
    ensure_marketparquet_catalog_activity,
    plan_marketparquet_stock_sync_activity,
    fetch_marketparquet_stock_days_activity,
    append_marketparquet_stock_days_activity,
    send_marketparquet_discord_message_activity,
)


async def run_worker() -> None:
    settings = Settings.from_env()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    with ThreadPoolExecutor(
        max_workers=4, thread_name_prefix="marketparquet-etl-activity"
    ) as activity_executor:
        worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=list(WORKFLOWS),
            activities=list(ACTIVITIES),
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
