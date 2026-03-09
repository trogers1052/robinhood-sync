"""Prometheus metrics for robinhood-sync."""

import logging
import os

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 9095

# ---------------------------------------------------------------------------
# Sync cycle metrics
# ---------------------------------------------------------------------------

SYNC_CYCLES = Counter(
    "robinhood_sync_cycles_total",
    "Total number of completed sync cycles",
)

CYCLE_DURATION = Histogram(
    "robinhood_sync_cycle_duration_seconds",
    "Duration of a full sync cycle in seconds",
    buckets=(1, 2, 5, 10, 20, 30, 60, 120, 300),
)

# ---------------------------------------------------------------------------
# Trade sync metrics
# ---------------------------------------------------------------------------

TRADES_SYNCED = Counter(
    "robinhood_sync_trades_synced_total",
    "Total number of new trades published to Kafka",
)

TRADES_SKIPPED = Counter(
    "robinhood_sync_trades_skipped_total",
    "Total number of already-synced trades skipped",
)

# ---------------------------------------------------------------------------
# Position / account metrics
# ---------------------------------------------------------------------------

POSITIONS = Gauge(
    "robinhood_sync_positions_total",
    "Current number of open positions",
)

BUYING_POWER = Gauge(
    "robinhood_sync_buying_power",
    "Current buying power in dollars",
)

WATCHLIST_SYMBOLS = Gauge(
    "robinhood_sync_watchlist_symbols",
    "Number of symbols on the watchlist",
)

# ---------------------------------------------------------------------------
# Robinhood API metrics
# ---------------------------------------------------------------------------

API_CALLS = Counter(
    "robinhood_api_calls_total",
    "Total Robinhood API calls by endpoint",
    ["endpoint"],
)

API_DURATION = Histogram(
    "robinhood_api_call_duration_seconds",
    "Robinhood API call latency in seconds",
    ["endpoint"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)

API_ERRORS = Counter(
    "robinhood_api_errors_total",
    "Total Robinhood API call failures by endpoint",
    ["endpoint"],
)

# ---------------------------------------------------------------------------
# Kafka metrics
# ---------------------------------------------------------------------------

KAFKA_PUBLISH_ERRORS = Counter(
    "robinhood_sync_kafka_publish_errors_total",
    "Total Kafka publish failures",
)

# ---------------------------------------------------------------------------
# Reliability metrics
# ---------------------------------------------------------------------------

CONSECUTIVE_FAILURES = Gauge(
    "robinhood_sync_consecutive_failures",
    "Current consecutive sync failure count",
)

LAST_SUCCESS = Gauge(
    "robinhood_sync_last_success_timestamp",
    "Unix timestamp of the last successful sync cycle",
)


def start_metrics_server() -> None:
    """Start Prometheus metrics HTTP server on METRICS_PORT (default 9095)."""
    port = int(os.environ.get("METRICS_PORT", str(_DEFAULT_PORT)))
    start_http_server(port)
    logger.info(f"Metrics server listening on :{port}/metrics")
