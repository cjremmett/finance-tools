from interest_rates_scraper.discord import build_discord_message
from interest_rates_scraper.models import (
    DiscordNotification,
    RateChange,
    SourceFailure,
)


def test_discord_message_formats_and_orders_changes() -> None:
    message = build_discord_message(
        DiscordNotification(
            workflow_run_id="run-123",
            baseline=False,
            changes=[
                RateChange(
                    source="kalshi",
                    product_key="kalshi",
                    product_name="Kalshi Account Balance",
                    change_type="added",
                    old_rate_percent=None,
                    new_rate_percent="3.4",
                ),
                RateChange(
                    source="marcus_cds",
                    product_key="marcus_cd:high_yield_cd:24",
                    product_name="Marcus 24-Month High-Yield CD",
                    change_type="added",
                    old_rate_percent=None,
                    new_rate_percent="3.7",
                ),
                RateChange(
                    source="federal_reserve",
                    product_key="fed",
                    product_name="Federal Funds Target Range",
                    change_type="changed",
                    old_rate_percent="3.4",
                    new_rate_percent="3.5",
                ),
                RateChange(
                    source="marcus_savings",
                    product_key="savings",
                    product_name="Marcus Savings",
                    change_type="added",
                    old_rate_percent=None,
                    new_rate_percent="3.4",
                ),
                RateChange(
                    source="marcus_cds",
                    product_key="marcus_cd:high_yield_cd:6",
                    product_name="Marcus 6-Month High-Yield CD",
                    change_type="added",
                    old_rate_percent=None,
                    new_rate_percent="3.95",
                ),
            ],
            failures=[SourceFailure(source="kalshi", error="parser changed")],
        )
    )

    assert "3.40% → 3.50%" in message
    assert "added at 3.40%" in message
    assert "**Marcus CDs**" in message
    assert "Marcus Cds" not in message
    assert message.index("**Federal Reserve**") < message.index("**Marcus Savings**")
    assert message.index("**Marcus Savings**") < message.index("**Marcus CDs**")
    assert message.index("**Marcus CDs**") < message.index("**Kalshi**")
    assert message.index("Marcus 6-Month") < message.index("Marcus 24-Month")
    assert "parser changed" in message
    assert "run-123" in message
