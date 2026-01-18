# Robinhood Sync Service

Automatically syncs your Robinhood trades to Kafka for processing by other services in the trading platform.

## Features

- Fetches all filled stock orders from Robinhood
- Publishes trade events to Kafka topic `trading.orders`
- Tracks synced orders to avoid duplicates
- Supports 2FA with TOTP
- Runs continuously with configurable polling interval
- Or run once for initial historical sync

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

# Run continuously (polls every 5 minutes by default)
python -m robinhood_sync.main

# Run with debug logging
python -m robinhood_sync.main --debug
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `ROBINHOOD_USERNAME` | Your Robinhood email | **Required** |
| `ROBINHOOD_PASSWORD` | Your Robinhood password | **Required** |
| `ROBINHOOD_TOTP_SECRET` | TOTP secret for 2FA | Optional |
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:19092` |
| `KAFKA_TOPIC` | Topic for trade events | `trading.orders` |
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_USER` | PostgreSQL user | `trader` |
| `DB_PASSWORD` | PostgreSQL password | `trader5` |
| `DB_NAME` | PostgreSQL database | `trading_platform` |
| `POLL_INTERVAL_SECONDS` | Polling interval | `300` (5 min) |
| `SYNC_HISTORY_DAYS` | Days of history on first run | `30` |

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
  -e DB_HOST=trading-db \
  robinhood-sync python -m robinhood_sync.main --once

# Run continuously
docker run -d \
  --name robinhood-sync \
  --network trading-network \
  --restart unless-stopped \
  -e ROBINHOOD_USERNAME=your_email \
  -e ROBINHOOD_PASSWORD=your_password \
  -e KAFKA_BROKERS=trading-redpanda:9092 \
  -e DB_HOST=trading-db \
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
│ • Skip duplicates   │
└──────────┬──────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐  ┌─────────────┐
│ Kafka   │  │ PostgreSQL  │
│ Topic   │  │ (tracking)  │
│         │  │             │
│ trading │  │ robinhood_  │
│ .orders │  │ synced_     │
│         │  │ orders      │
└─────────┘  └─────────────┘
     │
     ▼
┌─────────────────────┐
│  Trade Journal      │
│  Service            │
│  (consumer)         │
└─────────────────────┘
```

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

### "Challenge required" error
Robinhood may require additional verification:
1. Check your email for a verification code
2. Try logging in manually first via browser
3. Run the service with `--debug` for more details

### Orders not appearing
- Make sure orders are in "filled" state
- Check the date range with `--days` parameter
- Verify Kafka connection with `make kafka-check`

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black robinhood_sync/
isort robinhood_sync/
```

## Resources

- [robin-stocks Documentation](https://robin-stocks.readthedocs.io/)
- [robin-stocks GitHub](https://github.com/jmfernandes/robin_stocks)
- [Kafka Python Documentation](https://kafka-python.readthedocs.io/)
