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


def session_type(day: date, holidays: dict[date, str]) -> str:
    """Weekdays are open unless the holiday list says otherwise."""
    if day.weekday() >= 5:
        return "weekend"
    kind = holidays.get(day, "open")
    return _require_type(kind, day)


def previous_trading_session(today: date, holidays: dict[date, str]) -> date:
    """Latest session before today. That session belongs to the daily job."""
    for offset in range(1, _LOOKBACK_DAYS + 1):
        day = today - timedelta(days=offset)
        if session_type(day, holidays) in TRADING_TYPES:
            return day
    raise RuntimeError(
        f"no trading session in the {_LOOKBACK_DAYS} calendar days before {today.isoformat()}"
    )


def subtract_year(day: date) -> date:
    try:
        return day.replace(year=day.year - 1)
    except ValueError:
        return date(day.year - 1, 2, 28)


def backfill_sessions(
    excluded: date, holidays: dict[date, str]
) -> tuple[date, list[date]]:
    """Trading days from one calendar year before `excluded`, excluding that day."""
    start = subtract_year(excluded)
    days: list[date] = []
    cursor = start
    while cursor < excluded:
        if session_type(cursor, holidays) in TRADING_TYPES:
            days.append(cursor)
        cursor += timedelta(days=1)
    if not days:
        raise RuntimeError(
            f"backfill window {start.isoformat()}..{excluded.isoformat()} has no trading days"
        )
    return start, days


def publication_dates(sessions: list[date], excluded: date) -> dict[date, date]:
    """Open interest for a session is the print on the next trading morning."""
    if not sessions:
        raise ValueError("publication_dates requires at least one session")
    ordered = [*sessions, excluded]
    return {day: ordered[index + 1] for index, day in enumerate(sessions)}


def missing_runs(sessions: list[date], missing: list[date]) -> list[list[date]]:
    """Contiguous runs of `missing`, in `sessions` order."""
    wanted = set(missing)
    runs: list[list[date]] = []
    current: list[date] = []
    for day in sessions:
        if day in wanted:
            current.append(day)
            continue
        if current:
            runs.append(current)
            current = []
    if current:
        runs.append(current)
    return runs


def chunk_sessions(days: list[date], size: int) -> list[list[date]]:
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    return [days[index : index + size] for index in range(0, len(days), size)]


def coverage_holds(
    start: str, through: str, window_start: str, window_end: str
) -> bool:
    return start <= window_start and through >= window_end


def sessions_to_inspect(
    sessions: list[str],
    *,
    coverage_start: str | None,
    coverage_through: str | None,
    window_start: str,
    window_end: str,
) -> list[str]:
    """Days that still need a checkpoint check.

    A marker covering the window skips the symbol. A marker that covers a
    prefix of this window leaves only the sessions after `through`.
    """
    if not sessions:
        return []
    if (
        coverage_start is not None
        and coverage_through is not None
        and coverage_holds(coverage_start, coverage_through, window_start, window_end)
    ):
        return []
    if (
        coverage_start is not None
        and coverage_through is not None
        and coverage_start <= window_start
    ):
        return [day for day in sessions if day > coverage_through]
    return list(sessions)
