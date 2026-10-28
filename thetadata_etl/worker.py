from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

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
from thetadata_etl.config import Settings
from thetadata_etl.workflows import (
    ThetaDataOptionBackfillWorkflow,
    ThetaDataOptionDataWorkflow,
    ThetaDataSecuritiesWorkflow,
)

WORKFLOWS = (
    ThetaDataSecuritiesWorkflow,
    ThetaDataOptionDataWorkflow,
    ThetaDataOptionBackfillWorkflow,
)

ACTIVITIES = (
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
