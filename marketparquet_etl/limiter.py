from __future__ import annotations

import threading
import time
from collections.abc import Callable

# One token per authenticated MarketParquet API call. Presigned file GETs
# do not take a token. The free tier allows 60 requests per minute per key.
_BUCKET_SCRIPT = """
local now = tonumber(ARGV[1])
local rpm = tonumber(ARGV[2])
local tokens_key = 'marketparquet:tokens'
local ts_key = 'marketparquet:tokens_ts'

local tokens = redis.call('GET', tokens_key)
local ts = redis.call('GET', ts_key)
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
if tokens >= 1 then
  tokens = tokens - 1
  redis.call('SET', tokens_key, tostring(tokens))
  redis.call('SET', ts_key, tostring(now))
  return 0
end
redis.call('SET', tokens_key, tostring(tokens))
redis.call('SET', ts_key, tostring(now))
return math.ceil((1 - tokens) * 60000 / rpm)
""".strip()


class MemoryBucket:
    """In-process token bucket. Tests use this with a fake clock."""

    def __init__(
        self, rpm: int, now: Callable[[], float] | None = None
    ) -> None:
        if rpm < 1:
            raise ValueError("requests per minute must be positive")
        self.rpm = rpm
        self._now = now or time.monotonic
        self.tokens = float(rpm)
        self.updated = self._now()
        self._lock = threading.Lock()

    def _refill(self, now: float) -> None:
        elapsed_min = max(0.0, now - self.updated) / 60.0
        self.tokens = min(float(self.rpm), self.tokens + elapsed_min * self.rpm)
        self.updated = now

    def try_take(self) -> float:
        """Take one token, or return how many seconds to wait."""
        with self._lock:
            now = self._now()
            self._refill(now)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return 0.0
            missing = 1.0 - self.tokens
            return missing * 60.0 / self.rpm

    def acquire(self, sleep: Callable[[float], None] | None = None) -> None:
        sleeper = sleep or time.sleep
        while True:
            wait = self.try_take()
            if wait <= 0:
                return
            sleeper(wait)


class RedisBucket:
    """Shared bucket so overlapping runs stay under one key's limit."""

    def __init__(self, client: object, rpm: int) -> None:
        if rpm < 1:
            raise ValueError("requests per minute must be positive")
        self.rpm = rpm
        self._script = client.register_script(_BUCKET_SCRIPT)  # type: ignore[attr-defined]

    def try_take(self) -> float:
        wait_ms = int(self._script(args=[int(time.time() * 1000), self.rpm]))
        return max(0.0, wait_ms / 1000.0)

    def acquire(self, sleep: Callable[[float], None] | None = None) -> None:
        sleeper = sleep or time.sleep
        while True:
            wait = self.try_take()
            if wait <= 0:
                return
            sleeper(wait)


def redis_bucket(redis_url: str, rpm: int) -> RedisBucket:
    import redis

    client = redis.Redis.from_url(redis_url)
    return RedisBucket(client, rpm)
