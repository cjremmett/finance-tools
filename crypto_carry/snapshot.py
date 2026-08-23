from __future__ import annotations

from crypto_carry.carry import build_snapshot
from crypto_carry.coinbase import fetch_coinbase_spots
from crypto_carry.futures import fetch_cme_futures
from crypto_carry.models import CarrySnapshot


def fetch_carry_snapshot(
    observed_at: str, timeout_seconds: float
) -> CarrySnapshot:
    spots = fetch_coinbase_spots(timeout_seconds)
    quotes = fetch_cme_futures(timeout_seconds)
    return build_snapshot(observed_at, spots, quotes)
