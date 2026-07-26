from __future__ import annotations

from temporalio import activity

from kalshi_markets.database import mark_batch_sent, poll_and_stage
from kalshi_markets.discord import send_discord_message
from kalshi_markets.models import MarkBatchRequest, PollRequest, StageResult


@activity.defn(name="poll_and_stage_kalshi_markets")
def poll_and_stage_kalshi_markets_activity(request: PollRequest) -> StageResult:
    return poll_and_stage(request)


@activity.defn(name="send_kalshi_discord_message")
def send_kalshi_discord_message_activity(content: str) -> None:
    send_discord_message(content)


@activity.defn(name="mark_kalshi_notification_batch_sent")
def mark_kalshi_notification_batch_sent_activity(
    request: MarkBatchRequest,
) -> None:
    mark_batch_sent(request)

