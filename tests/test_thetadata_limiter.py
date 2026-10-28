import threading
import time

import pytest

from thetadata_etl.config import UNKNOWN_JOB_PRIORITY
from thetadata_etl.limiter import (
    _GATE_SCRIPT,
    _SEQUENCE_SCALE,
    MemoryGate,
    Waiter,
    new_state,
    priority_for,
    pump,
)

PRIORITIES = {"daily": 10, "backfill": 100}


def _gate() -> MemoryGate:
    return MemoryGate(
        max_concurrent=1,
        requests_per_minute=0,
        lease_seconds=30,
        priorities=PRIORITIES,
    )


def test_lower_priority_number_runs_first() -> None:
    state = new_state(
        max_concurrent=1, requests_per_minute=0, lease_seconds=900, now=0
    )
    state.waiters = [
        Waiter(100, 1, "earlier-backfill"),
        Waiter(10, 2, "later-daily"),
    ]
    assert pump(state, 0) == ["later-daily"]
    assert [waiter.request_id for waiter in state.waiters] == ["earlier-backfill"]


def test_fifo_within_one_priority() -> None:
    state = new_state(
        max_concurrent=2, requests_per_minute=0, lease_seconds=900, now=0
    )
    state.waiters = [Waiter(10, 2, "second"), Waiter(10, 1, "first")]
    assert pump(state, 0) == ["first", "second"]


def test_concurrency_cap_drops_a_nonblocking_waiter() -> None:
    gate = MemoryGate(
        max_concurrent=2,
        requests_per_minute=0,
        lease_seconds=30,
        priorities=PRIORITIES,
    )
    first = gate.acquire("daily", block=False)
    second = gate.acquire("daily", block=False)
    third = gate.acquire("backfill", block=False)
    assert first and second and third is None
    assert len(gate._state.leases) == 2
    assert gate._state.waiters == []


def test_release_wakes_the_next_waiter() -> None:
    gate = _gate()
    held = gate.acquire("backfill", block=False)
    assert held is not None
    result: dict[str, str] = {}

    def waiter() -> None:
        lease_id = gate.acquire("daily")
        assert lease_id is not None
        result["id"] = lease_id

    thread = threading.Thread(target=waiter)
    thread.start()
    deadline = time.time() + 2
    while time.time() < deadline and "id" not in result:
        with gate._lock:
            waiting = bool(gate._state.waiters)
        if waiting:
            break
        time.sleep(0.01)
    gate.release(held)
    thread.join(2)
    assert not thread.is_alive()
    assert result["id"] != held


def test_expired_lease_is_reclaimed() -> None:
    state = new_state(
        max_concurrent=1, requests_per_minute=0, lease_seconds=900, now=0
    )
    state.leases["old"] = 100
    state.waiters = [Waiter(10, 1, "next")]
    assert pump(state, 50) == []
    assert "old" in state.leases
    assert pump(state, 100) == ["next"]
    assert "old" not in state.leases


def test_per_minute_cap_spaces_grants() -> None:
    state = new_state(
        max_concurrent=2, requests_per_minute=60, lease_seconds=900, now=0
    )
    state.tokens = 0
    state.waiters = [Waiter(1, 1, "a"), Waiter(1, 2, "b")]
    assert pump(state, 0) == []
    assert pump(state, 1) == ["a"]
    assert pump(state, 1) == []
    assert pump(state, 2) == ["b"]


def test_unknown_job_sorts_behind_known_jobs() -> None:
    assert priority_for("NewJob", PRIORITIES) == UNKNOWN_JOB_PRIORITY


def test_lease_is_released_after_use() -> None:
    gate = _gate()
    with gate.lease("daily"):
        assert len(gate._state.leases) == 1
    assert gate._state.leases == {}
    with pytest.raises(RuntimeError, match="boom"):
        with gate.lease("daily"):
            raise RuntimeError("boom")
    assert gate._state.leases == {}


def test_redis_script_uses_the_same_grant_order() -> None:
    assert str(_SEQUENCE_SCALE) in _GATE_SCRIPT
    assert "thetadata:waiters" in _GATE_SCRIPT
    assert "thetadata:leases" in _GATE_SCRIPT
    assert "thetadata:inflight" in _GATE_SCRIPT
