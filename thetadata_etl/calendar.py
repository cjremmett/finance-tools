from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from thetadata_etl.datasets import EASTERN
from thetadata_etl.models import SessionResolution

SKIP_CLOSED_MORNING = (
    "open interest for the last session is published on the next trading morning"
)
TRADING_TYPES = frozenset({"open", "early_close"})
CALENDAR_TYPES = TRADING_TYPES | frozenset({"full_close", "weekend"})
_LOOKBACK_DAYS = 10


def _eastern(observed_at: str) -> datetime:
    moment = datetime.fromisoformat(observed_at)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=ZoneInfo("UTC"))
    return moment.astimezone(EASTERN)


def resolve_option_session(
    observed_at: str,
    *,
    schedule_type: Callable[[date], str],
) -> SessionResolution:
    moment = _eastern(observed_at)
    today = moment.date()
    if _require_type(schedule_type(today), today) not in TRADING_TYPES:
        return SessionResolution("", "", True, SKIP_CLOSED_MORNING)
    for offset in range(1, _LOOKBACK_DAYS + 1):
        day = today - timedelta(days=offset)
        if _require_type(schedule_type(day), day) in TRADING_TYPES:
            return SessionResolution(
                trade_date=day.isoformat(),
                oi_end_date=today.isoformat(),
                skipped=False,
                skip_reason="",
            )
    raise RuntimeError(
        f"no trading session in the {_LOOKBACK_DAYS} calendar days before {today.isoformat()}"
    )


def _require_type(value: str, day: date) -> str:
    if value not in CALENDAR_TYPES:
        raise RuntimeError(f"unexpected calendar type for {day.isoformat()}: {value}")
    return value
