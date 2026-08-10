# Robinhood Sync Service

Automatically syncs your Robinhood trades to Kafka for processing by other services in the trading platform.

## Features

- Fetches all filled stock orders from Robinhood
- Publishes trade events to Kafka topic `trading.orders`
- Tracks synced orders in Redis to avoid duplicates
- Also syncs positions, account balance, stop orders, watchlist, and the earnings calendar to Redis/Kafka
- Supports 2FA with TOTP
- Supports Docker secrets for credentials (`/run/secrets/`)
- **Durable authentication** — the OAuth session is persisted (Redis + file mirror) and renewed with the refresh token, so restarts resume instead of re-logging-in ([details](#authentication))
- Runs continuously with a configurable, market-hours-aware polling interval
- Or run once for an initial historical sync

## Quick Start

### 1. Install Dependencies

```bash
cd robinhood-sync
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Robinhood credentials
```

### 3. Run the Service

```bash
# Sync all trades once and exit
python -m robinhood_sync.main --once

# Sync only last 7 days
python -m robinhood_sync.main --once --days 7

# Run continuously (polls every 10 minutes by default, during market hours)
python -m robinhood_sync.main

# Run with debug logging
python -m robinhood_sync.main --debug
```

## Configuration

Configuration is loaded via `pydantic-settings` from environment variables (or a `.env` file). Credentials may alternatively be supplied as **Docker secrets** in `/run/secrets/` (`robinhood_username`, `robinhood_password`, `robinhood_totp_secret`), which take precedence over environment variables.

| Variable | Description | Default |
|----------|-------------|---------|
| `ROBINHOOD_USERNAME` | Your Robinhood email | **Required** |
| `ROBINHOOD_PASSWORD` | Your Robinhood password | **Required** |
| `ROBINHOOD_TOTP_SECRET` | TOTP secret for 2FA | Optional |
| `ROBINHOOD_DEVICE_TOKEN` | Pinned device identity (see [Authentication](#authentication)) | Auto-generated |
| `SESSION_REDIS_ENABLED` | Persist the session to Redis as well as the file mirror | `true` |
| `REDIS_SESSION_KEY` | Redis key holding the session document | `robinhood:session` |
| `ROBINHOOD_SESSION_FILE` | Local mirror of the session document | `~/.tokens/robinhood_session.json` |
| `SESSION_EXPIRES_IN` | Access token lifetime to request (seconds) | `86400` |
| `SESSION_REFRESH_RATIO` | Refresh once this fraction of the token's life has elapsed | `0.5` |
| `SESSION_REFRESH_MARGIN_SEC` | Always refresh with fewer than this many seconds left | `900` |
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:19092` |
| `KAFKA_TOPIC` | Topic for trade events | `trading.orders` |
| `KAFKA_POSITIONS_TOPIC` | Topic for position snapshots | `trading.positions` |
| `KAFKA_WATCHLIST_TOPIC` | Topic for watchlist events | `trading.watchlist` |
| `REDIS_HOST` | Redis host (synced-order tracking + caches) | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `REDIS_PASSWORD` | Redis password | Optional |
| `REDIS_DB` | Redis database number | `0` |
| `REDIS_SYNCED_ORDERS_KEY` | Redis set key for synced order IDs | `robinhood:synced_orders` |
| `WATCHLIST_NAMES` | Comma-separated Robinhood watchlist names to sync | `Materials` |
| `POLL_INTERVAL_MINUTES` | Polling interval during market hours (minutes) | `10` |
| `SYNC_HISTORY_DAYS` | Days of history on first run | `30` |
| `MARKET_OPEN_HOUR` | Market open hour, ET (pre-market start) | `4` |
| `MARKET_CLOSE_HOUR` | Market close hour, ET (after-hours end) | `20` |
| `TELEGRAM_BOT_TOKEN` | Bot token for halt alerts | Optional |
| `TELEGRAM_CHAT_ID` | Chat ID for halt alerts | Optional |

## Authentication

Authentication does **not** go through `rh.login()`. It lives in
`robinhood_sync/session.py`, because robin_stocks' own session handling loses
the login in three ways that cost this service two multi-week outages:

1. it stores the OAuth `refresh_token` but never uses it, so an expired access
   token falls straight back to a full password login;
2. a password login mints a **new random `device_token`** whenever its pickle
   can't be read — Robinhood sees an unknown device and issues an approval
   challenge that a headless service cannot answer;
3. the pickle is the only copy of the session, in a container volume, and is
   rewritten (`open(..., 'wb')`) on every attempt.

### How it works now

```
persisted session (Redis + file mirror)
        │
        ├─ resume ─────────────► valid?          ─► done (no network login)
        │                          │ no
        ├─ refresh_token grant ────┘              ─► done (rotated token persisted)
        │        │ rejected / no session
        └─ password login ────────────────────────► may raise a device challenge
```

The access token is refreshed at **half its life** (before each sync cycle and
at startup), so the password login is effectively never reached. The
`device_token` is pinned and persisted separately from the tokens, so even a
wiped store still presents the same trusted device.

A 429 never escalates to a password login — rate-limiting is treated as
"back off", not "re-authenticate". That is the guard against the restart-loop
that previously issued 205 login attempts in a row.

### Priming a session (the one interactive step)

Run this from a machine where you can approve the prompt or type an SMS code —
never on the headless host:

```bash
REDIS_HOST=<pi-host> REDIS_PASSWORD=... python -m robinhood_sync.prime_session
```

It logs in interactively, then writes the session (and device token) straight
into the Redis the service reads. Useful flags:

| Flag | Purpose |
|------|---------|
| `--reuse` | Resume/refresh the persisted session instead of logging in, if possible |
| `--force` | Always do a full password login |
| `--no-redis` | Write only the local file mirror |
| `--device-token TOKEN` | Pin a specific device identity |
| `--import-pickle PATH` | Adopt an existing robin_stocks pickle (migration) |

Record the device token it prints as a Docker secret
(`/run/secrets/robinhood_device_token`) so it survives even a wiped store.

## Trade Event Schema

Events published to Kafka have this structure:

```json
{
  "event_type": "TRADE_DETECTED",
  "source": "robinhood",
  "timestamp": "2026-01-17T15:30:00Z",
  "data": {
    "order_id": "abc123-def456",
    "symbol": "AAPL",
    "side": "buy",
    "quantity": "10.5",
    "average_price": "175.50",
    "total_notional": "1842.75",
    "fees": "0.00",
    "state": "filled",
    "executed_at": "2026-01-17T14:30:00Z",
    "created_at": "2026-01-17T14:29:00Z"
  }
}
```

## Docker

### Build

```bash
docker build -t robinhood-sync .
```

### Run

```bash
# Run once
docker run --rm \
  --network trading-network \
  -e ROBINHOOD_USERNAME=your_email \
  -e ROBINHOOD_PASSWORD=your_password \
  -e KAFKA_BROKERS=trading-redpanda:9092 \
  -e REDIS_HOST=trading-redis \
  robinhood-sync python -m robinhood_sync.main --once

# Run continuously
docker run -d \
  --name robinhood-sync \
  --network trading-network \
  --restart unless-stopped \
  -e ROBINHOOD_USERNAME=your_email \
  -e ROBINHOOD_PASSWORD=your_password \
  -e KAFKA_BROKERS=trading-redpanda:9092 \
  -e REDIS_HOST=trading-redis \
  robinhood-sync
```

## Architecture

```
┌─────────────────────┐
│   Robinhood API     │
│   (robin-stocks)    │
└──────────┬──────────┘
           │ get_all_stock_orders()
           ▼
┌─────────────────────┐
│  Robinhood Sync     │
│     Service         │
│                     │
│ • Parse orders      │
│ • Filter filled     │
│ • Skip duplicates   │  ◀── checks Redis set before publishing
└──────────┬──────────┘
           │
     ┌─────┴───────────┐
     ▼                 ▼
┌─────────┐  ┌──────────────────────┐
│ Kafka   │  │ Redis                │
│ Topic   │  │                      │
│         │  │ SET robinhood:       │
│ trading │  │   synced_orders      │  (dedup tracking)
│ .orders │  │ + positions,         │
│         │  │   watchlist,         │
│         │  │   stop orders,       │
│         │  │   earnings caches    │
└─────────┘  └──────────────────────┘
     │
     ▼
┌─────────────────────┐
│  Trade Journal      │
│  Service            │
│  (consumer)         │
└─────────────────────┘
```

**Deduplication:** every filled order's ID is checked against the Redis set
(`REDIS_SYNCED_ORDERS_KEY`, default `robinhood:synced_orders`) before publishing.
Once a trade is published to Kafka, its order ID is added to the set so it is
never re-emitted. No relational database is used.

## Security Notes

- Store credentials in `.env` file, never commit to git
- The `.env` file should be in `.gitignore`
- robin-stocks is an unofficial API - use at your own risk
- Consider using 2FA TOTP for automated login
- Robinhood may flag automated access - use reasonable polling intervals

## Troubleshooting

### Login fails with 2FA
If you have 2FA enabled, you need to provide the TOTP secret:
1. Disable 2FA temporarily in Robinhood app
2. Re-enable 2FA and choose "Authenticator app"
3. Copy the secret key shown during setup
4. Set `ROBINHOOD_TOTP_SECRET` in your `.env`

### "Challenge required" / `outcome=device_challenge`
The persisted session could not be resumed or refreshed, so the service fell
back to a password login and Robinhood challenged it. A headless host cannot
answer that, so the service **halts** rather than retry-looping (retrying is
what earns a rate-limit block).

Re-prime from a machine with a human present:

```bash
REDIS_HOST=<pi-host> REDIS_PASSWORD=... python -m robinhood_sync.prime_session
# approve the prompt on your phone, then restart the service
```

If this recurs often, check that `ROBINHOOD_DEVICE_TOKEN` is pinned and that
Redis is actually reachable from the service — a session that can't be
persisted has to be re-minted every restart.

### Session isn't surviving restarts
- `redis-cli GET robinhood:session` should return a document with a
  `refresh_token` and a `device_token`.
- The file mirror (`ROBINHOOD_SESSION_FILE`) must live on a mounted volume;
  the default `~/.tokens/` is volume-backed in the shipped compose.
- Look for `Resumed persisted Robinhood session` in the logs at startup. If you
  see `Password login to Robinhood as ...` instead, the store was empty.

### Orders not appearing
- Make sure orders are in "filled" state
- Check the date range with the `--days` parameter
- Verify the broker is reachable at `KAFKA_BROKERS` and that Redis is up at `REDIS_HOST:REDIS_PORT`
- An already-synced order will be skipped; inspect the Redis set (`SMEMBERS robinhood:synced_orders`) to confirm whether it was previously published

## Development

Dependencies (including the test tooling) are pinned in `requirements.txt` —
there is no `pyproject.toml`. Install them and run the test suite with `pytest`
(configured via `pytest.ini`):

```bash
# Install dependencies
pip install -r requirements.txt

# Run the test suite (config lives in pytest.ini)
pytest
```

## Resources

- [robin-stocks Documentation](https://robin-stocks.readthedocs.io/)
- [robin-stocks GitHub](https://github.com/jmfernandes/robin_stocks)
- [Kafka Python Documentation](https://kafka-python.readthedocs.io/)

---

## Built with Claude Code

A large portion of this project — implementation, tests, and documentation — was written in pair-programming sessions with [Claude Code](https://claude.com/claude-code), Anthropic's agentic command-line tool.
