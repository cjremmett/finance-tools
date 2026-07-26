from __future__ import annotations

import pytest

from interest_rates_scraper.scrapers.common import ScrapeError
from interest_rates_scraper.scrapers.fed import _latest_value, scrape_fed
from interest_rates_scraper.scrapers.kalshi import parse_kalshi
from interest_rates_scraper.scrapers.marcus import (
    parse_marcus_cds,
    parse_marcus_savings,
)


def test_fed_uses_latest_non_missing_value() -> None:
    payload = {
        "observations": [
            {"date": "2026-01-02", "value": "."},
            {"date": "2026-01-01", "value": "3.50"},
        ]
    }
    assert _latest_value(payload, "DFEDTARL") == ("2026-01-01", "3.5")


def test_fed_requires_api_key() -> None:
    with pytest.raises(ScrapeError, match="FRED_API_KEY is required"):
        scrape_fed(20, None)


def test_kalshi_parser_is_scoped_to_rate_sentence() -> None:
    result = parse_kalshi(
        """
        <article>
          <time>March 17, 2026</time>
          <h2>Interest Rates and Payments</h2>
          <p>The interest rate is set at 3.25% and applies to eligible accounts.</p>
          <p>Balances of $250 or more are eligible.</p>
        </article>
        """
    )
    assert result.source_effective_date == "2026-03-17"
    assert result.observations[0].rate_percent == "3.25"


def test_kalshi_parser_rejects_missing_rate() -> None:
    with pytest.raises(ScrapeError):
        parse_kalshi("<p>Interest rates are variable.</p>")


def test_marcus_savings_parser() -> None:
    result = parse_marcus_savings(
        """
        <main>
          <div>3.40% APY Online Savings Account</div>
          <small>Annual Percentage Yield (APY) as of July 21, 2026.</small>
        </main>
        """
    )
    assert result.source_effective_date == "2026-07-21"
    assert result.observations[0].rate_percent == "3.4"


def test_marcus_cd_parser_normalizes_terms_and_types() -> None:
    result = parse_marcus_cds(
        """
        <h1>Marcus CD Rates</h1>
        <p>CD rates as of July 20, 2026</p>
        <table>
          <tr><td>6 months</td><td>High-Yield CD</td><td>3.95%</td><td>$500</td><td>Yes</td></tr>
          <tr><td>11 months</td><td>No-Penalty CD</td><td>4.00%</td><td>$500</td><td>No</td></tr>
          <tr><td>2 years</td><td>High-Yield CD</td><td>3.70%</td><td>$500</td><td>Yes</td></tr>
          <tr><td>20-Month</td><td>Rate Bump CD</td><td>3.75%</td><td>$500</td><td>Yes</td></tr>
        </table>
        """
    )
    assert result.source_effective_date == "2026-07-20"
    assert {item.term_months for item in result.observations} == {6, 11, 20, 24}
    assert {
        item.product_type for item in result.observations
    } == {"High-Yield CD", "No-Penalty CD", "Rate Bump CD"}
    assert [item.term_months for item in result.observations] == [6, 11, 20, 24]


def test_marcus_cd_parser_rejects_empty_or_partial_page() -> None:
    with pytest.raises(ScrapeError):
        parse_marcus_cds(
            "<p>6 months High-Yield CD 3.95% $500 Yes</p>"
        )
