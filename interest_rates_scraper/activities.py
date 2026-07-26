from __future__ import annotations

from temporalio import activity

from interest_rates_scraper.config import Settings
from interest_rates_scraper.database import mark_notification_sent, persist_scrape
from interest_rates_scraper.discord import send_discord
from interest_rates_scraper.models import (
    DiscordNotification,
    PersistRequest,
    PersistResult,
    ScrapeResult,
)
from interest_rates_scraper.scrapers.fed import scrape_fed
from interest_rates_scraper.scrapers.kalshi import scrape_kalshi
from interest_rates_scraper.scrapers.marcus import (
    scrape_marcus_cds,
    scrape_marcus_savings,
)


@activity.defn(name="scrape_fed_rate")
def scrape_fed_rate_activity() -> ScrapeResult:
    return scrape_fed(Settings.from_env().http_timeout_seconds)


@activity.defn(name="scrape_kalshi_apy")
def scrape_kalshi_apy_activity() -> ScrapeResult:
    return scrape_kalshi(Settings.from_env().http_timeout_seconds)


@activity.defn(name="scrape_marcus_savings")
def scrape_marcus_savings_activity() -> ScrapeResult:
    return scrape_marcus_savings(Settings.from_env().http_timeout_seconds)


@activity.defn(name="scrape_marcus_cds")
def scrape_marcus_cds_activity() -> ScrapeResult:
    return scrape_marcus_cds(Settings.from_env().http_timeout_seconds)


@activity.defn(name="persist_interest_rates")
def persist_interest_rates_activity(request: PersistRequest) -> PersistResult:
    return persist_scrape(request)


@activity.defn(name="send_discord_notification")
def send_discord_notification_activity(notification: DiscordNotification) -> None:
    send_discord(notification)


@activity.defn(name="mark_notification_sent")
def mark_notification_sent_activity(workflow_run_id: str) -> None:
    mark_notification_sent(workflow_run_id)
