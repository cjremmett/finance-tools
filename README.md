# Finance Tools

This repository contains a shared Temporal worker pool that monitors:

- The Federal Reserve's target federal funds range.
- Kalshi's APY for eligible prediction-account cash and open positions.
- Marcus Online Savings Account APY.
- Every CD rate currently advertised by Marcus.
- X Money Premium+ APY.
- Newly created Kalshi prediction markets in configured categories.
- Coinbase BTC/ETH spot prices and simple carry APYs for listed CME Bitcoin
  and Ether futures.
- Daily ThetaData stock symbols and option underlyings, stored as dated CSVs
  in Garage and appended to the `securities_data` Iceberg warehouse.
- The previous session's option prices, volume, and open interest for symbols
  that have both a stock listing and an options chain.
- One year of that same option history for those symbols, excluding the latest
  session, which the daily option load still owns.

The worker process registers every workflow and all of their activities on the
single `finance-tools` task queue. The interest-rate workflow runs its five
source activities concurrently, saves every successful result to PostgreSQL,
and sends a Discord webhook message when rates or products change. Successful
sources are retained even if another source exhausts its retries.

## Configuration

Copy `.env.example` to `.env` and set:

```dotenv
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
FRED_API_KEY=your-fred-api-key
```

The Federal Reserve activity uses the official FRED API. Keep its API key in
`.env`; that file is excluded from Git.

The default `TEMPORAL_ADDRESS=host.docker.internal:7233` connects from the
worker container to a Temporal frontend published on port 7233 of the same
Linux host. Override it if Temporal is available under another hostname.

Every replica in the worker pool registers this complete Temporal contract:

| Workflow type | Task queue | Input |
| --- | --- | --- |
| `InterestRateScrapeWorkflow` | `finance-tools` | None |
| `KalshiMarketMonitorWorkflow` | `finance-tools` | None |
| `CryptoCarryWorkflow` | `finance-tools` | None |
| `ThetaDataSecuritiesWorkflow` | `finance-tools` | None |
| `ThetaDataOptionDataWorkflow` | `finance-tools` | None |
| `ThetaDataOptionBackfillWorkflow` | `finance-tools` | None |

The namespace defaults to `default`. Do not run workers with different workflow
registrations on the shared queue; every `finance-tools` worker must register
every workflow type and every activity. Temporal Schedules are configured
externally through the CLI rather than managed by this repository.

## Run with Docker

```bash
docker compose up --build -d
docker compose logs -f worker
```

The Compose project starts PostgreSQL 18, starts Redis for the shared ThetaData
request gate, applies Alembic migrations, and then starts one combined worker
service. Redis listens on the compose network at `redis://redis:6379/0`. Scale
the worker service when additional worker capacity or redundancy is needed:

```bash
docker compose up -d --scale worker=2
```

PostgreSQL is published on host port `11001` for other tools and projects in
this repository:

```text
Host: <Docker host address>
Port: 11001
Database: finance_tools
Username: finance-tools
Password: finance-tools
```

The scraper owns the `interest_rates` schema inside the `finance_tools`
database. Other projects in this repository can use their own schemas while
remaining available for ordinary cross-schema queries.

The Kalshi market monitor owns the separate `kalshi_markets` schema. It stores
only its latest polling window and a singleton delivery checkpoint; it does not
retain market history. It also keeps a small category registry seeded with all
categories known when the feature was introduced.

To start one check manually with the Temporal CLI:

```bash
temporal workflow start \
  --type InterestRateScrapeWorkflow \
  --task-queue finance-tools \
  --workflow-id "interest-rates-$(date +%s)"
```

## Kalshi market monitor

`KalshiMarketMonitorWorkflow` runs on the same `finance-tools` task queue:

```bash
temporal workflow start \
  --type KalshiMarketMonitorWorkflow \
  --task-queue finance-tools \
  --workflow-id "kalshi-markets-$(date +%s)"
```

Configure the shared worker with:

```dotenv
KALSHI_API_KEY_ID=your-read-only-key-id
KALSHI_PRIVATE_KEY_BASE64=base64-encoded-private-key-pem
KALSHI_DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
KALSHI_CATEGORY_WHITELIST=Economics,Elections,Financials,Politics,Science and Technology
KALSHI_EVENT_RESOLUTION_TIMEOUT_SECONDS=60
KALSHI_MIN_MARKET_DURATION_DAYS=0
```

The first run establishes a cutoff without backfilling old markets. Later runs
query markets created since the last successful cutoff, exclude multivariate
markets, resolve canonical categories through Kalshi series, and send one or
more Discord messages. A successful run with no eligible additions still sends
a status message. If Kalshi publishes a category outside the seeded registry,
the worker stores it and sends a separate category-release message; the new
category remains excluded from market alerts until it is explicitly added to
`KALSHI_CATEGORY_WHITELIST`. Exhausted polling retries send a failure message
and leave the cutoff unchanged.

`KALSHI_MIN_MARKET_DURATION_DAYS` filters markets whose scheduled trading
window (`open_time` through `close_time`) is shorter than the configured number
of 24-hour days. The default `0` disables the filter, decimal values are
supported, and a market exactly equal to the threshold is retained. Markets
with missing or invalid duration timestamps are retained and logged so
incomplete Kalshi metadata does not silently suppress an alert. Kalshi may
move `close_time` earlier after creation, so this filter applies only to the
schedule reported when the market is first discovered.

The polling window's upper bound is captured when the workflow starts. If
Kalshi returns a new market before its event metadata becomes visible, the
worker retains that market response and retries only the unresolved events
with exponential backoff for up to
`KALSHI_EVENT_RESOLUTION_TIMEOUT_SECONDS` (one minute by default). Markets
whose events remain unresolved are ignored, while markets with resolved events
continue through category filtering and notification. A
successful run saves the original upper bound as its next cutoff, so markets
created while event resolution is waiting are included in the following run.

The downloaded key files under `kalshi_markets/` are not read by the worker.
After putting the key ID and a base64-encoded PEM in `.env`, delete those local
plaintext files manually if they are no longer needed.

## Crypto carry digest

`CryptoCarryWorkflow` runs on the same `finance-tools` task queue:

```bash
temporal workflow start \
  --type CryptoCarryWorkflow \
  --task-queue finance-tools \
  --workflow-id "crypto-carry-$(date +%s)"
```

Configure the shared worker with:

```dotenv
CRYPTO_CARRY_DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

Each run fetches Coinbase BTC-USD and ETH-USD spots and delayed CME Bitcoin
and Ether futures quotes from TradingView's public futures scanner. It reports
the simple carry APY of every listed standard BTC and ETH contract with at
least seven days to expiry, picking long-spot/short-futures or
short-spot/long-futures so the APY is non-negative. Fees, margin interest, and
spread are ignored. Micros, Friday weeklies, euro-quoted, ratio, and volatility
contracts are excluded.

APY uses spot notional: `((F - S) / S) * 365 / days`, where `days` is the
actual time remaining until CME last trade (16:00 Europe/London on the
contract's expiration date). Front-month APYs are omitted when fewer than
seven days remain because they are dominated by a short tenor rather than
the basis.

CME crypto futures cash-settle to CF Benchmarks (BRR / ETHUSD_RR), not
Coinbase, so this is a theoretical basis screen rather than a locked delivery
arb. CME Group's own delayed-quote pages block automated access; the scanner
is unofficial and delayed. Schema drift or a missing asset fails the run and
sends a Discord failure message after Temporal exhausts three activity
attempts. Successful runs always post the digest; there is no database
snapshot.

## ThetaData securities snapshot

`ThetaDataSecuritiesWorkflow` runs on the same `finance-tools` task queue:

```bash
temporal workflow start \
  --type ThetaDataSecuritiesWorkflow \
  --task-queue finance-tools \
  --workflow-id "thetadata-securities-$(date +%s)"
```

Configure the shared worker with:

```dotenv
THETADATA_API_KEY=your-thetadata-api-key
GARAGE_ACCESS_KEY_ID=your-garage-access-key
GARAGE_SECRET_ACCESS_KEY=your-garage-secret-key
QUACK_TOKEN=your-quack-token
```

Each run uses the America/New_York calendar day. For `stocks` and
`option_underlyings`, it checks Garage for
`s3://securities-data/securities/<dataset>/YYYY-MM-DD.csv`. A missing object
is fetched from ThetaData, given a `date` column, and uploaded. The workflow
then attaches the `securities_data` Lakekeeper warehouse through the DuckDB
server on port 9494, creates `securities_data.securities` and the symbol
tables when they are absent, and inserts that day's CSV. DuckDB writes the
Iceberg data files as Parquet. A date already present in the table is not
inserted again. Both symbol-list calls take a lease from the shared ThetaData
gate described below.

## ThetaData option chains

`ThetaDataOptionDataWorkflow` loads the previous trading session's option
prices, volume, and open interest for every symbol present in both the latest
Garage stock snapshot and the latest option-underlying snapshot.

```bash
temporal workflow start \
  --type ThetaDataOptionDataWorkflow \
  --task-queue finance-tools \
  --workflow-id "thetadata-option-data-$(date +%s)"
```

The workflow asks ThetaData's calendar which session to store. The `date`
column is that session. A weekend or full-market holiday morning finishes
successfully and leaves the table as it is, because that morning does not
publish the prior session's open interest.

On a trading morning, before any contracts are written, one activity loads
open interest for the highest-volume symbol stored before this trade date.
An empty volume history uses AAPL. ThetaData returns no data when today's
print is not up yet. A print whose timestamps fall on the trade date is the
previous session. Either result fails that activity. Its Temporal retry
policy waits 5 minutes and tries again, up to 6 attempts. The first attempt
runs immediately, so the last attempt is 25 minutes after the start. The
activity's schedule-to-close limit is 30 minutes.

Open interest is stored as `0` when ThetaData sends zero or omits the print
for a symbol after that probe has succeeded. That includes a contract with
no print and a symbol whose whole chain is absent. Thin names are saved that
way.

Symbols go out in descending order of total option volume on the latest stored
session before this trade date. Ties break alphabetically by symbol. The first
run, with an empty volume history, is alphabetical. Each batch is appended to
`securities_data.securities.option_data` when its checkpoints are written, and
the per-symbol totals land in
`securities_data.securities.option_symbol_volume`. After the full session is
stored, the workflow writes
`securities/option_data/YYYY-MM-DD/_COMPLETE`. The next run for that session
reads the marker and returns the stored row count.

## ThetaData option history backfill

`ThetaDataOptionBackfillWorkflow` fills price, volume, and open interest for
every symbol in that same stock and option-underlying join. The window is the
trading sessions from one calendar year before the latest session, up to but
not including that latest session. Weekends and full closures are skipped.
Early closes are included. The latest session stays with the 7:00 a.m. job.

```bash
temporal workflow start \
  --type ThetaDataOptionBackfillWorkflow \
  --task-queue finance-tools \
  --workflow-id thetadata-option-backfill-daily
```

Each symbol is requested a few sessions at a time. A range that comes back
short, or that ThetaData rejects, is split, and a single session uses the same
loader as the daily job. Empty chains are stored as empty checkpoints. Rows
already in `option_data` for that symbol and date are not inserted again.
`option_symbol_volume` gets the same daily totals. A symbol is finished only
when every session in the window has a checkpoint. The next morning fills
symbols that are new to the join and any session the daily job missed once
that session is no longer the latest one. A morning with nothing missing does
no ThetaData work.

Progress is the per-session checkpoint under
`securities/option_data/YYYY-MM-DD/symbols/` plus
`securities/option_backfill/{symbol}.json` written after the Iceberg append.
The workflow passes its place through `continue-as-new`. Stopping this
compose, or restarting the worker, leaves the Temporal run open so it
continues when the worker is back. Redis does not store that progress.
Terminating the workflow and starting it again reads the checkpoints and
continues from the symbol-days that are still missing. A symbol that still
fails after five data attempts is left unmarked and retried on the next run.

The backfill acquires the shared gate as `ThetaDataOptionBackfillWorkflow`
(priority 100). While a morning job is waiting, the backfill parks between
calls.

## Temporal Schedules

Compose runs Redis on the compose network, and every ThetaData call in this
repository takes a lease from it before the request. Worker replicas share the
gate, so a long historical backfill and the morning jobs stay inside the plan's
concurrency limit. `THETADATA_MAX_CONCURRENT` defaults to 2.
`THETADATA_REQUESTS_PER_MINUTE` defaults to 0, which leaves the per-minute
bucket off. `THETADATA_JOB_PRIORITIES` sets the order. A lower number runs
first:

```dotenv
REDIS_URL=redis://redis:6379/0
THETADATA_MAX_CONCURRENT=2
THETADATA_REQUESTS_PER_MINUTE=0
THETADATA_JOB_PRIORITIES=ThetaDataOptionDataWorkflow=10,ThetaDataSecuritiesWorkflow=20,ThetaDataOptionBackfillWorkflow=100
```

The daily option load runs ahead of the symbol snapshot, and both run ahead of
`ThetaDataOptionBackfillWorkflow`. Calls already in flight finish. The morning
job then takes the open slots. A backfill call keeps its lease until the
response returns, refreshing the deadline so a slow week does not lose the slot
while it is still running.

## Temporal Schedules

The following schedules run interest-rate checks hourly on the hour, the
Kalshi, crypto-carry, and ThetaData securities jobs daily at 5:00 a.m. Eastern,
the ThetaData option chain job daily at 7:00 a.m. Eastern, and the option
history backfill daily at 8:30 a.m. Eastern. Overlap `Skip` on the backfill
leaves an 8:30 trigger alone while an earlier backfill run is still open:

```bash
temporal schedule create \
  --schedule-id interest-rates-hourly \
  --cron "0 * * * *" \
  --overlap-policy Skip \
  --workflow-id interest-rates-hourly \
  --type InterestRateScrapeWorkflow \
  --task-queue finance-tools

temporal schedule create \
  --schedule-id kalshi-markets-daily \
  --cron "0 5 * * *" \
  --time-zone "America/New_York" \
  --overlap-policy Skip \
  --workflow-id kalshi-markets-daily \
  --type KalshiMarketMonitorWorkflow \
  --task-queue finance-tools

temporal schedule create \
  --schedule-id crypto-carry-daily \
  --cron "0 5 * * *" \
  --time-zone "America/New_York" \
  --overlap-policy Skip \
  --workflow-id crypto-carry-daily \
  --type CryptoCarryWorkflow \
  --task-queue finance-tools

temporal schedule create \
  --schedule-id thetadata-securities-daily \
  --cron "0 5 * * *" \
  --time-zone "America/New_York" \
  --overlap-policy Skip \
  --workflow-id thetadata-securities-daily \
  --type ThetaDataSecuritiesWorkflow \
  --task-queue finance-tools

temporal schedule create \
  --schedule-id thetadata-option-data-daily \
  --cron "0 7 * * *" \
  --time-zone "America/New_York" \
  --overlap-policy Skip \
  --workflow-id thetadata-option-data-daily \
  --type ThetaDataOptionDataWorkflow \
  --task-queue finance-tools

temporal schedule create \
  --schedule-id thetadata-option-backfill-daily \
  --cron "30 8 * * *" \
  --time-zone "America/New_York" \
  --overlap-policy Skip \
  --workflow-id thetadata-option-backfill-daily \
  --type ThetaDataOptionBackfillWorkflow \
  --task-queue finance-tools
```

Add `--address` and `--namespace` when the CLI is not already configured for
the intended Temporal frontend and namespace.

## Local development

Install dependencies and run the test suite with `uv`:

```bash
uv sync
uv run pytest
```

Parser tests use local HTML and JSON fixtures and never contact the live
financial sites. Database behavior is tested with SQLite; deployment uses
PostgreSQL, including a transaction-level advisory lock that serializes
snapshot comparison.

## Data and failure behavior

Rates are stored as exact decimal percentages. Each run records successful and
failed sources, every successful observation, the source's advertised effective
date when available, and notification state.

A complete successful source response is compared with that source's previous
complete snapshot:

- New products are reported as additions.
- Changed APYs are shown as old value to new value.
- Missing products are reported as removals.
- An empty or structurally suspicious page fails parsing and is never treated
  as removal of every product.

The initial successful run sends a baseline message. Source failures are
reported to Discord after Temporal exhausts three activity attempts.
