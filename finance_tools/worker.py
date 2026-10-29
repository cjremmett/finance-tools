from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from crypto_carry.activities import (
    fetch_crypto_carry_activity,
    send_crypto_carry_discord_message_activity,
)
from crypto_carry.workflows import CryptoCarryWorkflow
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
from interest_rates_scraper.workflows import InterestRateScrapeWorkflow
from kalshi_markets.activities import (
    mark_kalshi_notification_batch_sent_activity,
    poll_and_stage_kalshi_markets_activity,
    send_kalshi_discord_message_activity,
)
from kalshi_markets.workflows import KalshiMarketMonitorWorkflow
from marketparquet_etl.activities import (
    append_marketparquet_stock_days_activity,
    ensure_marketparquet_catalog_activity,
    fetch_marketparquet_stock_days_activity,
    plan_marketparquet_stock_sync_activity,
    send_marketparquet_discord_message_activity,
)
from marketparquet_etl.workflows import MarketParquetStockDataWorkflow
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
    send_thetadata_discord_message_activity,
    symbol_csv_exists_activity,
)
from thetadata_etl.workflows import (
    ThetaDataOptionBackfillWorkflow,
    ThetaDataOptionDataWorkflow,
    ThetaDataSecuritiesWorkflow,
)

WORKFLOWS = (
    InterestRateScrapeWorkflow,
    KalshiMarketMonitorWorkflow,
    CryptoCarryWorkflow,
    ThetaDataSecuritiesWorkflow,
    ThetaDataOptionDataWorkflow,
    ThetaDataOptionBackfillWorkflow,
    MarketParquetStockDataWorkflow,
)

ACTIVITIES = (
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
    send_thetadata_discord_message_activity,
    ensure_marketparquet_catalog_activity,
    plan_marketparquet_stock_sync_activity,
    fetch_marketparquet_stock_days_activity,
    append_marketparquet_stock_days_activity,
    send_marketparquet_discord_message_activity,
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
        max_workers=16,
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

