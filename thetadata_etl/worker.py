from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from thetadata_etl.activities import (
    append_symbols_to_iceberg_activity,
    ensure_securities_catalog_activity,
    fetch_and_upload_symbols_activity,
    resolve_symbol_fetch_date_activity,
    symbol_csv_exists_activity,
)
from thetadata_etl.config import Settings
from thetadata_etl.workflows import ThetaDataSecuritiesWorkflow


async def run_worker() -> None:
    settings = Settings.from_env()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    with ThreadPoolExecutor(
        max_workers=4, thread_name_prefix="thetadata-etl-activity"
    ) as activity_executor:
        worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=[ThetaDataSecuritiesWorkflow],
            activities=[
                resolve_symbol_fetch_date_activity,
                ensure_securities_catalog_activity,
                symbol_csv_exists_activity,
                fetch_and_upload_symbols_activity,
                append_symbols_to_iceberg_activity,
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
