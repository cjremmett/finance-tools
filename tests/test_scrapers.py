from __future__ import annotations

import pytest

from interest_rates_scraper.scrapers.common import ScrapeError
from interest_rates_scraper.scrapers.fed import _latest_value, scrape_fed
from interest_rates_scraper.scrapers.kalshi import parse_kalshi
from interest_rates_scraper.scrapers.marcus import (
    parse_marcus_cds,
    parse_marcus_savings,
)
from interest_rates_scraper.scrapers.x_money import parse_x_money


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


def test_kalshi_parser_reads_prediction_market_rate_only() -> None:
    result = parse_kalshi(
        """
        <span><time datetime="2026-10-20T23:46:09Z">Updated today</time></span>
        <article>
          <h1>Prediction Market Interest</h1>
          <p><b>Rate:</b> 3.50% APY. This rate is variable and can change at any time.</p>
          <p>Balances of $250 or more are eligible.</p>
          <h1>Perpetuals (Perps) Interest</h1>
          <p><b>Rate:</b> 4.25% APY. This rate is variable and can change at any time.</p>
          <h1>Earn 7% APY on Perpetuals (Perps)</h1>
          <p>Perps traders can now earn 7% APY. That's double our standard 3.50% rate.</p>
        </article>
        """
    )
    assert result.source == "kalshi"
    assert result.source_effective_date == "2026-10-20"
    observation = result.observations[0]
    assert observation.product_key == "kalshi_predictions_apy"
    assert observation.product_name == "Kalshi Cash and Open Positions APY"
    assert observation.rate_percent == "3.5"
    assert observation.source_effective_date == "2026-10-20"


def test_kalshi_parser_falls_back_to_written_date() -> None:
    result = parse_kalshi(
        """
        <article>
          <time>March 17, 2026</time>
          <h1>Prediction Market Interest</h1>
          <p>Rate: 3.25% APY.</p>
        </article>
        """
    )
    assert result.source_effective_date == "2026-03-17"
    assert result.observations[0].rate_percent == "3.25"


def test_kalshi_parser_rejects_missing_rate() -> None:
    with pytest.raises(ScrapeError, match="Prediction Market Interest"):
        parse_kalshi("<p>Interest rates are variable.</p>")


def test_kalshi_parser_rejects_section_without_rate() -> None:
    with pytest.raises(ScrapeError, match="prediction-market APY"):
        parse_kalshi(
            """
            <article>
              <h1>Prediction Market Interest</h1>
              <p>Rates are variable.</p>
            </article>
            """
        )


def test_kalshi_parser_rejects_ambiguous_prediction_rate() -> None:
    with pytest.raises(ScrapeError, match="prediction-market APY"):
        parse_kalshi(
            """
            <article>
              <h1>Prediction Market Interest</h1>
              <p>Rate: 3.50% APY.</p>
              <p>Rate: 3.75% APY.</p>
            </article>
            """
        )


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


_X_MONEY_RATES_HTML = """
<main>
  <h1>X Money Stored Value Account Rates</h1>
  <table>
    <tr>
      <td>The X Subscription</td>
      <td>Free or Plus</td>
      <td>Premium, Super, Ultra</td>
    </tr>
    <tr>
      <td>Legacy X Premium</td>
      <td>Basic, Premium, or no subscription</td>
      <td>Premium+</td>
    </tr>
    <tr>
      <td>Standard Annual Percentage Yield ("APY")*</td>
      <td>4.00%</td>
      <td>6.00%</td>
    </tr>
    <tr>
      <td>Standard Interest Rate</td>
      <td>3.92%</td>
      <td>5.83%</td>
    </tr>
    <tr>
      <td>Boosted Annual Percentage Yield ("APY")**</td>
      <td>6.00%</td>
      <td>N/A</td>
    </tr>
    <tr>
      <td>Boosted Interest Rate**</td>
      <td>5.83%</td>
      <td>N/A</td>
    </tr>
  </table>
  <p>
    *Annual Percentage Yield ("APY") and interest rate is effective as of
    10/12/2026. APY and interest rate are variable and subject to change
    at any time.
  </p>
</main>
"""


def test_x_money_parser_reads_premium_plus_apy() -> None:
    result = parse_x_money(_X_MONEY_RATES_HTML)
    assert result.source == "x_money"
    assert result.source_effective_date == "2026-10-12"
    assert [
        (item.product_key, item.product_name, item.rate_percent)
        for item in result.observations
    ] == [
        ("x_money_premium_plus_apy", "X Money Premium+ APY", "6"),
    ]
    assert all(item.product_type == "savings_apy" for item in result.observations)


def test_x_money_parser_rejects_missing_table() -> None:
    with pytest.raises(ScrapeError, match="rates table was not found"):
        parse_x_money("<p>X Money pays up to 6.00% APY.</p>")


def test_x_money_parser_rejects_na_for_required_apy() -> None:
    html = _X_MONEY_RATES_HTML.replace("<td>6.00%</td>", "<td>N/A</td>", 1)
    with pytest.raises(ScrapeError, match="numeric APY"):
        parse_x_money(html)


def test_x_money_parser_rejects_partial_table() -> None:
    with pytest.raises(ScrapeError, match="Standard APY"):
        parse_x_money(
            """
            <table>
              <tr>
                <td>The X Subscription</td>
                <td>Free or Plus</td>
                <td>Premium, Super, Ultra</td>
              </tr>
              <tr>
                <td>Legacy X Premium</td>
                <td>Basic, Premium, or no subscription</td>
                <td>Premium+</td>
              </tr>
            </table>
            """
        )
