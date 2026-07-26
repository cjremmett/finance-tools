from interest_rates_scraper.discord import build_discord_message
from interest_rates_scraper.models import (
    DiscordNotification,
    RateChange,
    SourceFailure,
)


def test_discord_message_contains_changes_and_failures() -> None:
    message = build_discord_message(
        DiscordNotification(
            workflow_run_id="run-123",
            baseline=False,
            changes=[
                RateChange(
                    source="marcus_savings",
                    product_key="savings",
                    product_name="Marcus Savings",
                    change_type="changed",
                    old_rate_percent="3.4",
                    new_rate_percent="3.5",
                )
            ],
            failures=[SourceFailure(source="kalshi", error="parser changed")],
        )
    )
    assert "3.4% → 3.5%" in message
    assert "parser changed" in message
    assert "run-123" in message
