# Finance Tools

This repository contains Temporal workers that monitor:

- The Federal Reserve's target federal funds range.
- Kalshi's APY for eligible prediction-account cash and open positions.
- Marcus Online Savings Account APY.
- Every CD rate currently advertised by Marcus.
- Newly created Kalshi prediction markets in configured categories.

Each source is scraped in an independent Temporal activity. A workflow runs all
four activities concurrently, saves every successful result to PostgreSQL, and
sends a Discord webhook message when rates or products change. Successful
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

The worker registers this public Temporal contract:

| Setting | Default |
| --- | --- |
| Workflow type | `InterestRateScrapeWorkflow` |
| Task queue | `interest-rates` |
| Namespace | `default` |
| Workflow input | None |

Temporal Schedules are intentionally not managed by this repository.

## Run with Docker

```bash
docker compose up --build -d
docker compose logs -f worker
```

The Compose project starts PostgreSQL 18, applies Alembic migrations, and then
starts the worker. PostgreSQL is published on host port `11001` for other tools
and projects in this repository:

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
  --task-queue interest-rates \
  --workflow-id "interest-rates-$(date +%s)"
```

## Kalshi market monitor

The `KalshiMarketMonitorWorkflow` runs on the `kalshi-markets` task queue by
default. Temporal Schedules remain external to this repository, so the workflow
may be started daily, hourly, or at any other cadence:

```bash
temporal workflow start \
  --type KalshiMarketMonitorWorkflow \
  --task-queue kalshi-markets \
  --workflow-id "kalshi-markets-$(date +%s)"
```

Configure the dedicated worker with:

```dotenv
KALSHI_API_KEY_ID=your-read-only-key-id
KALSHI_PRIVATE_KEY_BASE64=base64-encoded-private-key-pem
KALSHI_DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
KALSHI_CATEGORY_WHITELIST=Economics,Elections,Financials,Politics,Science and Technology
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

The downloaded key files under `kalshi_markets/` are not read by the worker.
After putting the key ID and a base64-encoded PEM in `.env`, delete those local
plaintext files manually if they are no longer needed.

## Local development

Install dependencies and run the test suite with `uv`:

```bash
uv sync
uv run pytest
```

Parser tests use local HTML fixtures and never contact the live financial
sites. Database behavior is tested with SQLite; deployment uses PostgreSQL,
including a transaction-level advisory lock that serializes snapshot
comparison.

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
