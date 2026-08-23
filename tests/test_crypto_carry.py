from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from crypto_carry.carry import MIN_DAYS_TO_EXPIRY, build_snapshot, carry_row
from crypto_carry.config import Settings
from crypto_carry.coinbase import parse_coinbase_spot
from crypto_carry.common import ScrapeError
from crypto_carry.discord import (
    DISCORD_CONTENT_LIMIT,
    build_digest_messages,
    build_failure_message,
)
from crypto_carry.futures import parse_futures_scan
from crypto_carry.models import CarryRow, CarrySnapshot, FuturesQuote, SpotPrice
from crypto_carry.workflows import CryptoCarryWorkflow


def _spot(asset: str, price: str) -> SpotPrice:
    return SpotPrice(
        asset=asset,
        price=price,
        source="coinbase",
        source_url=f"https://api.coinbase.com/v2/prices/{asset}-USD/spot",
    )


def _quote(
    asset: str,
    month_code: str,
    year: str,
    *,
    expiration: str,
    last_trade_at: str,
    price: str,
) -> FuturesQuote:
    contract_code = f"{asset}{month_code}{year}"
    month_names = {
        "F": "JAN",
        "G": "FEB",
        "H": "MAR",
        "J": "APR",
        "K": "MAY",
        "M": "JUN",
        "N": "JUL",
        "Q": "AUG",
        "U": "SEP",
        "V": "OCT",
        "X": "NOV",
        "Z": "DEC",
    }
    return FuturesQuote(
        asset=asset,
        symbol=f"CME:{contract_code}",
        contract_code=contract_code,
        month_label=f"{month_names[month_code]} {year}",
        expiration=expiration,
        last_trade_at=last_trade_at,
        price=price,
    )


def _row(
    symbol: str, close: float | None, description: str, expiration: int | None
) -> dict[str, object]:
    return {
        "s": symbol,
        "d": [symbol.split(":")[-1], close, description, expiration, "CME"],
    }


SCAN_FIXTURE = {
    "data": [
        _row("CME:BTC1!", 77800, "Bitcoin Futures", None),
        _row(
            "CME:MBTQ2026",
            77785,
            "Micro Bitcoin Futures (Aug 2026)",
            20260828,
        ),
        _row(
            "CME:BTEZ2026",
            68305,
            "Bitcoin Euro Futures (Dec 2026)",
            20261224,
        ),
        _row("CME:BTCQ2026", 77800, "Bitcoin Futures (Aug 2026)", 20260828),
        _row("CME:BTCU2026", 77925, "Bitcoin Futures (Sep 2026)", 20260925),
        _row("CME:BTCZ2027", 82775, "Bitcoin Futures (Dec 2027)", 20271231),
        _row("CME:ETHU2026", 2426, "Ether Futures (Sep 2026)", 20260925),
        _row("CME:ETHZ2027", 2610.5, "Ether Futures (Dec 2027)", 20271231),
        _row("CME:METU2026", 2478.5, "Micro Ether Futures (Sep 2026)", 20260925),
    ]
}


def test_futures_parser_keeps_standard_btc_and_eth_only() -> None:
    quotes = parse_futures_scan(SCAN_FIXTURE)
    codes = {quote.contract_code for quote in quotes}
    assert codes == {
        "BTCQ2026",
        "BTCU2026",
        "BTCZ2027",
        "ETHU2026",
        "ETHZ2027",
    }
    august = next(quote for quote in quotes if quote.contract_code == "BTCQ2026")
    assert august.expiration == "2026-08-28"
    assert august.last_trade_at == "2026-08-28T16:00:00+01:00"
    assert august.price == "77800"
    december = next(quote for quote in quotes if quote.contract_code == "BTCZ2027")
    assert december.last_trade_at == "2027-12-31T16:00:00+00:00"
    assert december.month_label == "DEC 2027"


def test_futures_parser_rejects_missing_close_on_matching_symbol() -> None:
    payload = {
        "data": [
            _row("CME:BTCU2026", None, "Bitcoin Futures (Sep 2026)", 20260925)
        ]
    }
    with pytest.raises(ScrapeError, match="missing close"):
        parse_futures_scan(payload)


def test_futures_parser_rejects_missing_expiration_on_matching_symbol() -> None:
    payload = {
        "data": [_row("CME:BTCU2026", 77925, "Bitcoin Futures (Sep 2026)", None)]
    }
    with pytest.raises(ScrapeError, match="expiration"):
        parse_futures_scan(payload)


def test_futures_parser_rejects_scanner_error() -> None:
    with pytest.raises(ScrapeError, match="scanner error"):
        parse_futures_scan({"errmsg": "bad columns", "data": []})


def test_futures_parser_ignores_non_standard_contracts() -> None:
    payload = {
        "data": [
            _row(
                "CME:MBTQ2026",
                77785,
                "Micro Bitcoin Futures (Aug 2026)",
                20260828,
            ),
            _row("CME:BTC1!", 77800, "Bitcoin Futures", None),
        ]
    }
    assert parse_futures_scan(payload) == []


def test_futures_parser_rejects_zero_close() -> None:
    payload = {
        "data": [_row("CME:BTCU2026", 0, "Bitcoin Futures (Sep 2026)", 20260925)]
    }
    with pytest.raises(ScrapeError, match="non-positive close"):
        parse_futures_scan(payload)


def test_futures_parser_rejects_empty_data_type() -> None:
    with pytest.raises(ScrapeError, match="missing data list"):
        parse_futures_scan({"data": {}})


def test_coinbase_parser_reads_amount() -> None:
    spot = parse_coinbase_spot(
        {"data": {"amount": "77392.685", "base": "BTC", "currency": "USD"}},
        "BTC",
        "https://api.coinbase.com/v2/prices/BTC-USD/spot",
    )
    assert spot.price == "77392.685"
    assert spot.asset == "BTC"


def test_coinbase_parser_rejects_missing_amount() -> None:
    with pytest.raises(ScrapeError, match="missing Coinbase spot amount"):
        parse_coinbase_spot({"data": {}}, "BTC", "https://example.test")


def test_coinbase_parser_rejects_invalid_amount() -> None:
    with pytest.raises(ScrapeError, match="invalid Coinbase spot amount"):
        parse_coinbase_spot(
            {"data": {"amount": "n/a"}},
            "BTC",
            "https://example.test",
        )


def test_coinbase_parser_rejects_non_positive_amount() -> None:
    with pytest.raises(ScrapeError, match="non-positive"):
        parse_coinbase_spot(
            {"data": {"amount": "0"}},
            "ETH",
            "https://example.test",
        )


def test_carry_simple_contango_is_one_percent_over_a_year() -> None:
    observed = datetime(2026, 1, 1, tzinfo=timezone.utc)
    quote = _quote(
        "BTC",
        "Z",
        "2026",
        expiration="2027-01-01",
        last_trade_at="2027-01-01T00:00:00+00:00",
        price="101",
    )
    row = carry_row(quote, _spot("BTC", "100"), observed)
    assert row is not None
    assert row.direction == "long/short"
    assert Decimal(row.apy_percent) == Decimal("1")


def test_carry_backwardation_flips_direction_and_reports_abs_apy() -> None:
    observed = datetime(2026, 1, 1, tzinfo=timezone.utc)
    quote = _quote(
        "ETH",
        "Z",
        "2026",
        expiration="2027-01-01",
        last_trade_at="2027-01-01T00:00:00+00:00",
        price="99",
    )
    row = carry_row(quote, _spot("ETH", "100"), observed)
    assert row is not None
    assert row.direction == "short/long"
    assert Decimal(row.apy_percent) == Decimal("1")


def test_carry_skips_contracts_inside_one_week() -> None:
    observed = datetime(2026, 8, 23, 9, 0, tzinfo=timezone.utc)
    just_under = _quote(
        "BTC",
        "Q",
        "2026",
        expiration="2026-08-30",
        last_trade_at=(observed + timedelta(days=7) - timedelta(seconds=1)).isoformat(),
        price="78000",
    )
    exactly_week = _quote(
        "BTC",
        "U",
        "2026",
        expiration="2026-08-30",
        last_trade_at=(observed + timedelta(days=7)).isoformat(),
        price="78000",
    )
    assert carry_row(just_under, _spot("BTC", "77000"), observed) is None
    kept = carry_row(exactly_week, _spot("BTC", "77000"), observed)
    assert kept is not None
    assert Decimal(kept.days_remaining) == MIN_DAYS_TO_EXPIRY


def test_build_snapshot_requires_a_week_plus_contract_per_asset() -> None:
    observed = datetime(2026, 8, 23, 9, 0, tzinfo=timezone.utc)
    near = _quote(
        "BTC",
        "Q",
        "2026",
        expiration="2026-08-28",
        last_trade_at=(observed + timedelta(days=3)).isoformat(),
        price="77800",
    )
    far_eth = _quote(
        "ETH",
        "U",
        "2026",
        expiration="2026-09-25",
        last_trade_at=(observed + timedelta(days=30)).isoformat(),
        price="2500",
    )
    with pytest.raises(ScrapeError, match="no BTC futures"):
        build_snapshot(
            observed.isoformat(),
            [_spot("BTC", "77000"), _spot("ETH", "2400")],
            [near, far_eth],
        )


def test_build_snapshot_orders_rows_by_asset_and_expiry() -> None:
    observed = datetime(2026, 8, 23, 9, 0, tzinfo=timezone.utc)
    later = observed + timedelta(days=40)
    earlier = observed + timedelta(days=20)
    snapshot = build_snapshot(
        observed.isoformat(),
        [_spot("ETH", "2400"), _spot("BTC", "77000")],
        [
            _quote(
                "ETH",
                "Z",
                "2026",
                expiration="2026-12-24",
                last_trade_at=later.isoformat(),
                price="2500",
            ),
            _quote(
                "BTC",
                "Z",
                "2026",
                expiration="2026-12-24",
                last_trade_at=later.isoformat(),
                price="80000",
            ),
            _quote(
                "BTC",
                "U",
                "2026",
                expiration="2026-09-25",
                last_trade_at=earlier.isoformat(),
                price="78000",
            ),
            _quote(
                "ETH",
                "U",
                "2026",
                expiration="2026-09-25",
                last_trade_at=earlier.isoformat(),
                price="2450",
            ),
        ],
    )
    assert [row.contract_code for row in snapshot.rows] == [
        "BTCU2026",
        "BTCZ2026",
        "ETHU2026",
        "ETHZ2026",
    ]


def _digest_snapshot() -> CarrySnapshot:
    observed = datetime(2026, 8, 23, 9, 0, tzinfo=timezone.utc)
    return CarrySnapshot(
        observed_at=observed.isoformat(),
        spots=[_spot("ETH", "2456.33"), _spot("BTC", "77392.685")],
        rows=[
            CarryRow(
                asset="BTC",
                contract_code="BTCU2026",
                month_label="SEP 2026",
                expiration="2026-09-25",
                days_remaining="33.2",
                futures_price="77925",
                spot_price="77392.685",
                apy_percent="6.104",
                direction="long/short",
            ),
            CarryRow(
                asset="BTC",
                contract_code="BTCZ2026",
                month_label="DEC 2026",
                expiration="2026-12-24",
                days_remaining="123.4",
                futures_price="78455",
                spot_price="77392.685",
                apy_percent="4.05",
                direction="long/short",
            ),
            CarryRow(
                asset="ETH",
                contract_code="ETHU2026",
                month_label="SEP 2026",
                expiration="2026-09-25",
                days_remaining="33.2",
                futures_price="2426",
                spot_price="2456.33",
                apy_percent="13.5",
                direction="short/long",
            ),
        ],
    )


def test_discord_digest_formats_spots_and_contracts() -> None:
    messages = build_digest_messages(_digest_snapshot(), "run-123")
    assert len(messages) == 1
    message = messages[0]
    assert message.startswith("📈 **Crypto carry** — <t:")
    assert message.index("**Spot (Coinbase)**") < message.index("**Bitcoin**")
    assert message.index("**Bitcoin**") < message.index("**Ether**")
    assert message.index("SEP 2026") < message.index("DEC 2026")
    assert "• BTC $77,392.69" in message
    assert "• ETH $2,456.33" in message
    assert "F $77,925" in message
    assert "**6.10%**" in message
    assert "long/short" in message
    assert "short/long" in message
    assert "run-123" in message
    assert "long spot, short futures" not in message
    assert "I know what it means" not in message


def test_discord_failure_message_truncates_error() -> None:
    message = build_failure_message("run-9", "parser changed\n" + ("x" * 5000))
    assert message.startswith("⚠️ **Crypto carry failed after retries**")
    assert "run-9" in message
    assert "parser changed" in message
    assert len(message) <= DISCORD_CONTENT_LIMIT
    assert "\n" not in message.split("Error: ", 1)[1]


def test_discord_digest_splits_when_over_limit() -> None:
    observed = datetime(2026, 8, 23, 9, 0, tzinfo=timezone.utc)
    rows = [
        CarryRow(
            asset="BTC" if index < 40 else "ETH",
            contract_code=f"{'BTC' if index < 40 else 'ETH'}Z{2027 + index}",
            month_label=f"DEC {2027 + index} extra padding " + ("X" * 40),
            expiration=f"{2027 + index}-12-31",
            days_remaining="400",
            futures_price="80000",
            spot_price="77000",
            apy_percent="5.5",
            direction="long/short",
        )
        for index in range(50)
    ]
    snapshot = CarrySnapshot(
        observed_at=observed.isoformat(),
        spots=[_spot("BTC", "77000"), _spot("ETH", "2400")],
        rows=rows,
    )
    messages = build_digest_messages(snapshot, "run-split")
    assert len(messages) > 1
    assert all(len(message) <= DISCORD_CONTENT_LIMIT for message in messages)
    assert all("run-split" in message for message in messages)
    assert all(message.startswith("📈 **Crypto carry**") for message in messages)


def test_settings_require_discord_webhook(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CRYPTO_CARRY_DISCORD_WEBHOOK_URL", raising=False)
    with pytest.raises(RuntimeError, match="CRYPTO_CARRY_DISCORD_WEBHOOK_URL"):
        Settings.from_env()


def test_workflow_passes_temporal_sandbox_validation() -> None:
    async def validate() -> None:
        SandboxedWorkflowRunner().prepare_workflow(
            workflow._Definition.must_from_class(CryptoCarryWorkflow)
        )

    asyncio.run(validate())
