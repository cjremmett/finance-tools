from __future__ import annotations

import contextvars
import logging
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field

from thetadata_etl.config import UNKNOWN_JOB_PRIORITY, Settings

LOGGER = logging.getLogger("thetadata_etl.limiter")

# Redis sorted-set scores are float64. priority * 2^33 + sequence stays exact
# for priorities up to about a million and sequences up to 2^33.
_SEQUENCE_SCALE = 2**33
_WAKE_PREFIX = "thetadata:wake:"
_WAITER_DEADLINE_MS = 30_000
_HOLD_SECONDS = 10

_GATE_SCRIPT = f"""
local command = ARGV[1]
local now = tonumber(ARGV[2])
local max_concurrent = tonumber(ARGV[3])
local rpm = tonumber(ARGV[4])
local lease_ms = tonumber(ARGV[5])
local request_id = ARGV[6]
local priority = tonumber(ARGV[7])

local waiters = 'thetadata:waiters'
local leases = 'thetadata:leases'
local deadlines = 'thetadata:waiter_deadline'
local seq_key = 'thetadata:seq'
local tokens_key = 'thetadata:tokens'
local tokens_ts_key = 'thetadata:tokens_ts'

local function reclaim()
  local expired = redis.call('ZRANGEBYSCORE', leases, '-inf', now)
  if #expired > 0 then
    redis.call('ZREM', leases, unpack(expired))
  end
  local stale = redis.call('ZRANGEBYSCORE', deadlines, '-inf', now)
  if #stale > 0 then
    redis.call('ZREM', waiters, unpack(stale))
    redis.call('ZREM', deadlines, unpack(stale))
  end
end

local function refill()
  if rpm <= 0 then
    return
  end
  local tokens = redis.call('GET', tokens_key)
  local ts = redis.call('GET', tokens_ts_key)
  if tokens == false then
    tokens = rpm
  else
    tokens = tonumber(tokens)
  end
  if ts == false then
    ts = now
  else
    ts = tonumber(ts)
  end
  local elapsed_min = math.max(0, now - ts) / 60000
  tokens = math.min(rpm, tokens + elapsed_min * rpm)
  redis.call('SET', tokens_key, tostring(tokens))
  redis.call('SET', tokens_ts_key, tostring(now))
end

local function tokens_ok()
  if rpm <= 0 then
    return true
  end
  return tonumber(redis.call('GET', tokens_key)) >= 1
end

local function take_token()
  if rpm <= 0 then
    return
  end
  local tokens = tonumber(redis.call('GET', tokens_key))
  redis.call('SET', tokens_key, tostring(tokens - 1))
end

local function enqueue(id)
  local seq = redis.call('INCR', seq_key)
  local score = priority * {_SEQUENCE_SCALE} + seq
  redis.call('ZADD', waiters, score, id)
  redis.call('ZADD', deadlines, now + {_WAITER_DEADLINE_MS}, id)
end

local function grant()
  while redis.call('ZCARD', leases) < max_concurrent and tokens_ok() do
    local top = redis.call('ZPOPMIN', waiters, 1)
    if top[1] == nil then
      break
    end
    local id = top[1]
    redis.call('ZREM', deadlines, id)
    redis.call('ZADD', leases, now + lease_ms, id)
    take_token()
    redis.call('LPUSH', 'thetadata:wake:' .. id, id)
    redis.call('EXPIRE', 'thetadata:wake:' .. id, 120)
  end
  redis.call('SET', 'thetadata:inflight', redis.call('ZCARD', leases))
end

-- A refresh from the holder arrives before reclaim, so a lease whose
-- deadline has passed stays owned while that call is still running.
if command == 'refresh' and redis.call('ZSCORE', leases, request_id) then
  redis.call('ZADD', leases, now + lease_ms, request_id)
end

reclaim()
refill()

if command == 'register' then
  if redis.call('ZSCORE', waiters, request_id) == false and redis.call('ZSCORE', leases, request_id) == false then
    enqueue(request_id)
  elseif redis.call('ZSCORE', waiters, request_id) then
    redis.call('ZADD', deadlines, now + {_WAITER_DEADLINE_MS}, request_id)
  end
elseif command == 'refresh' then
  if redis.call('ZSCORE', leases, request_id) then
    -- deadline was moved forward above
  elseif redis.call('ZSCORE', waiters, request_id) then
    redis.call('ZADD', deadlines, now + {_WAITER_DEADLINE_MS}, request_id)
  else
    enqueue(request_id)
  end
elseif command == 'release' then
  redis.call('ZREM', leases, request_id)
  redis.call('ZREM', deadlines, request_id)
else
  return redis.error_reply('unknown gate command')
end

grant()
if redis.call('ZSCORE', leases, request_id) then
  return 1
end
return 0
"""


@contextmanager
def _refresh_until_released(
    refresh: Callable[[], None],
    heartbeat: Callable[[], None] | None,
):
    stop = threading.Event()

    def run() -> None:
        while not stop.wait(_HOLD_SECONDS):
            try:
                refresh()
            except Exception:
                LOGGER.warning("ThetaData lease refresh failed", exc_info=True)
            if heartbeat is None:
                continue
            try:
                heartbeat()
            except Exception:
                LOGGER.warning("ThetaData lease heartbeat failed", exc_info=True)

    context = contextvars.copy_context()
    thread = threading.Thread(
        target=context.run,
        args=(run,),
        name="thetadata-lease-refresh",
        daemon=True,
    )
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=_HOLD_SECONDS + 1)


def priority_for(job_name: str, priorities: dict[str, int]) -> int:
    if job_name not in priorities:
        LOGGER.warning(
            "ThetaData job %s has no priority; using %s",
            job_name,
            UNKNOWN_JOB_PRIORITY,
        )
        return UNKNOWN_JOB_PRIORITY
    return priorities[job_name]


@dataclass
class Waiter:
    priority: int
    sequence: int
    request_id: str


@dataclass
class GateState:
    max_concurrent: int
    requests_per_minute: int
    lease_seconds: float
    waiters: list[Waiter] = field(default_factory=list)
    leases: dict[str, float] = field(default_factory=dict)
    tokens: float = 0.0
    tokens_at: float = 0.0
    sequence: int = 0


def new_state(
    *,
    max_concurrent: int,
    requests_per_minute: int,
    lease_seconds: float,
    now: float,
) -> GateState:
    tokens = float(requests_per_minute) if requests_per_minute > 0 else 0.0
    return GateState(
        max_concurrent=max_concurrent,
        requests_per_minute=requests_per_minute,
        lease_seconds=lease_seconds,
        tokens=tokens,
        tokens_at=now,
    )


def pump(state: GateState, now: float) -> list[str]:
    expired = [
        lease_id for lease_id, expiry in state.leases.items() if expiry <= now
    ]
    for lease_id in expired:
        del state.leases[lease_id]
    if state.requests_per_minute > 0:
        elapsed_min = max(0.0, now - state.tokens_at) / 60.0
        state.tokens = min(
            float(state.requests_per_minute),
            state.tokens + elapsed_min * state.requests_per_minute,
        )
        state.tokens_at = now
    state.waiters.sort(key=lambda waiter: (waiter.priority, waiter.sequence))
    granted: list[str] = []
    while len(state.leases) < state.max_concurrent and state.waiters:
        if state.requests_per_minute > 0 and state.tokens < 1:
            break
        waiter = state.waiters.pop(0)
        if state.requests_per_minute > 0:
            state.tokens -= 1
        state.leases[waiter.request_id] = now + state.lease_seconds
        granted.append(waiter.request_id)
    return granted


class MemoryGate:
    def __init__(
        self,
        *,
        max_concurrent: int,
        requests_per_minute: int,
        lease_seconds: float,
        priorities: dict[str, int],
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._priorities = priorities
        self._state = new_state(
            max_concurrent=max_concurrent,
            requests_per_minute=requests_per_minute,
            lease_seconds=lease_seconds,
            now=self._clock(),
        )
        self._lock = threading.Lock()
        self._events: dict[str, threading.Event] = {}

    def acquire(
        self,
        job_name: str,
        *,
        block: bool = True,
        heartbeat: Callable[[], None] | None = None,
    ) -> str | None:
        priority = priority_for(job_name, self._priorities)
        request_id = uuid.uuid4().hex
        event = threading.Event()
        with self._lock:
            self._state.sequence += 1
            self._state.waiters.append(
                Waiter(priority, self._state.sequence, request_id)
            )
            self._events[request_id] = event
            self._signal(pump(self._state, self._clock()))
            if request_id in self._state.leases:
                return request_id
            if not block:
                self._state.waiters = [
                    waiter
                    for waiter in self._state.waiters
                    if waiter.request_id != request_id
                ]
                self._events.pop(request_id, None)
                return None
        last_heartbeat = self._clock()
        while True:
            if event.wait(timeout=0.05):
                return request_id
            now = self._clock()
            if heartbeat is not None and now - last_heartbeat >= 10:
                heartbeat()
                last_heartbeat = now
            with self._lock:
                self._signal(pump(self._state, self._clock()))
                if request_id in self._state.leases:
                    return request_id

    def extend(self, lease_id: str) -> None:
        with self._lock:
            if lease_id in self._state.leases:
                self._state.leases[lease_id] = (
                    self._clock() + self._state.lease_seconds
                )

    def release(self, lease_id: str) -> None:
        with self._lock:
            self._state.leases.pop(lease_id, None)
            self._events.pop(lease_id, None)
            self._signal(pump(self._state, self._clock()))

    def _signal(self, granted: list[str]) -> None:
        for lease_id in granted:
            event = self._events.get(lease_id)
            if event is not None:
                event.set()

    @contextmanager
    def lease(self, job_name: str, heartbeat: Callable[[], None] | None = None):
        lease_id = self.acquire(job_name, heartbeat=heartbeat)
        if lease_id is None:
            raise RuntimeError("ThetaData lease was not granted")
        try:
            with _refresh_until_released(lambda: self.extend(lease_id), heartbeat):
                yield lease_id
        finally:
            self.release(lease_id)


class RedisGate:
    def __init__(self, client: object, settings: Settings) -> None:
        self._redis = client
        self._settings = settings
        self._script = client.register_script(_GATE_SCRIPT)

    def _invoke(self, command: str, request_id: str, priority: int) -> int:
        now_ms = int(time.time() * 1000)
        return int(
            self._script(
                args=[
                    command,
                    now_ms,
                    self._settings.thetadata_max_concurrent,
                    self._settings.thetadata_requests_per_minute,
                    self._settings.thetadata_lease_seconds * 1000,
                    request_id,
                    priority,
                ]
            )
        )

    def acquire(
        self,
        job_name: str,
        *,
        block: bool = True,
        heartbeat: Callable[[], None] | None = None,
    ) -> str | None:
        del block
        priority = priority_for(job_name, self._settings.thetadata_job_priorities)
        request_id = uuid.uuid4().hex
        if self._invoke("register", request_id, priority) == 1:
            return self._finish(request_id)
        last_heartbeat = time.monotonic()
        while True:
            item = self._redis.blpop(f"{_WAKE_PREFIX}{request_id}", timeout=10)
            if item is not None:
                return self._finish(request_id)
            now = time.monotonic()
            if heartbeat is not None and now - last_heartbeat >= 10:
                heartbeat()
                last_heartbeat = now
            if self._invoke("refresh", request_id, priority) == 1:
                return self._finish(request_id)

    def extend(self, lease_id: str) -> None:
        self._invoke("refresh", lease_id, 0)

    def release(self, lease_id: str) -> None:
        self._invoke("release", lease_id, 0)

    def close(self) -> None:
        self._redis.close()

    def _finish(self, request_id: str) -> str:
        self._redis.delete(f"{_WAKE_PREFIX}{request_id}")
        return request_id

    @contextmanager
    def lease(self, job_name: str, heartbeat: Callable[[], None] | None = None):
        lease_id = self.acquire(job_name, heartbeat=heartbeat)
        try:
            if lease_id is None:
                yield None
                return
            with _refresh_until_released(
                lambda: self.extend(lease_id), heartbeat
            ):
                yield lease_id
        finally:
            if lease_id is not None:
                self.release(lease_id)


def redis_gate(settings: Settings) -> RedisGate:
    import redis

    client = redis.Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=20,
    )
    return RedisGate(client, settings)
