"""
Robinhood Sync Service - Main Entry Point

Syncs trades from Robinhood to Kafka for processing by other services.
Runs continuously during market hours (Mon-Fri, 4am-8pm ET).
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from dotenv import load_dotenv

from .config import get_settings, Settings
from .metrics import (
    start_metrics_server,
    SYNC_CYCLES,
    CYCLE_DURATION,
    CONSECUTIVE_FAILURES,
    LAST_SUCCESS,
)
from .robinhood_client import LoginOutcome
from .sync import TradeSyncService
from .scheduler import MarketScheduler
from .telegram_notifier import TelegramNotifier


# Login retry policy. Tuned around Robinhood's behavior:
#   - 429s on get_prompts_status decay over hours, not minutes — short
#     backoff just keeps the rate-limit bucket full
#   - Transient blips (DNS, 5xx, container start race) clear in minutes
#   - A device-approval challenge requires a human; no amount of waiting fixes it
_TRANSIENT_BACKOFF_BASE_SEC = 600       # 10 minutes
_TRANSIENT_BACKOFF_CAP_SEC = 21_600     # 6 hours
_TRANSIENT_MAX_ATTEMPTS = 6
_RATE_LIMIT_COOLDOWN_SEC = 14_400       # 4 hours
_RATE_LIMIT_MAX_ATTEMPTS = 3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
_shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global _shutdown_requested
    logger.info(f"Received signal {signum}, initiating shutdown...")
    _shutdown_requested = True


def run_once(settings: Settings, since_days: Optional[int] = None) -> int:
    """
    Run a single sync operation.

    Args:
        settings: Application settings.
        since_days: Only sync trades from the last N days.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    service = TradeSyncService(settings)
    logger.info("Go Bears!!!!")

    try:
        if not service.initialize():
            logger.error("Failed to initialize service")
            return 1

        import time as _time
        _start = _time.monotonic()
        new_synced, skipped = service.sync_trades(since_days=since_days)
        # Also sync current positions
        service.sync_positions()
        # Sync watchlist
        added, removed = service.sync_watchlist()
        # Sync stop orders
        stop_count = service.sync_stop_orders()
        # Sync earnings calendar
        earnings_count = service.sync_earnings_calendar()
        _duration = _time.monotonic() - _start

        SYNC_CYCLES.inc()
        CYCLE_DURATION.observe(_duration)
        LAST_SUCCESS.set_to_current_time()
        CONSECUTIVE_FAILURES.set(0)

        logger.info(f"Sync complete: {new_synced} new trades, {skipped} already synced")
        logger.info(f"Watchlist: {added} added, {removed} removed")
        logger.info(f"Stop orders: {stop_count} synced")
        logger.info(f"Earnings calendar: {earnings_count} symbols processed")
        return 0

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        CONSECUTIVE_FAILURES.set(1)
        return 1

    finally:
        service.cleanup()


def _interruptible_sleep(seconds: float) -> None:
    """Sleep that respects the shutdown flag, in 10s increments."""
    slept = 0.0
    while slept < seconds and not _shutdown_requested:
        nap = min(10.0, seconds - slept)
        time.sleep(nap)
        slept += nap


def _halt_with_alert(
    notifier: TelegramNotifier,
    outcome: LoginOutcome,
    detail: str,
) -> None:
    """
    Send a Telegram alert describing the unrecoverable login failure, then
    log a final message. Caller is responsible for exiting.
    """
    if outcome == LoginOutcome.DEVICE_CHALLENGE:
        title = "Robinhood device approval required"
        action = (
            "The persisted session could not be resumed or refreshed, so a "
            "password login was attempted and Robinhood challenged it. "
            "Re-prime from your Mac:\n"
            "  cd ~/Projects/trading-platform/robinhood-sync\n"
            "  REDIS_HOST=<pi-host> python -m robinhood_sync.prime_session\n"
            "Approve the prompt on your phone, then "
            "`docker start projects-robinhood-sync-1`."
        )
    elif outcome == LoginOutcome.BAD_CREDENTIALS:
        title = "Robinhood login rejected (bad credentials?)"
        action = (
            "Verify ROBINHOOD_USERNAME / ROBINHOOD_PASSWORD / "
            "ROBINHOOD_TOTP_SECRET in the Pi's .env, then restart "
            "the container."
        )
    else:
        title = f"Robinhood login halted: {outcome.value}"
        action = (
            "Login retry budget exhausted. Inspect docker logs "
            "for projects-robinhood-sync-1 and intervene manually."
        )

    message = (
        f"🚨 robinhood-sync halted\n\n"
        f"{title}\n\n"
        f"{detail}\n\n"
        f"Action required:\n{action}\n\n"
        f"The container will stay stopped until you restart it. "
        f"This is intentional — auto-retrying past this point is what "
        f"caused the prior 1,000+ attempt rate-limit incident."
    )
    logger.error(f"HALTING login retries: {title} — {detail}")
    notifier.send(message)


def _initialize_with_retries(
    service: TradeSyncService,
    notifier: TelegramNotifier,
) -> LoginOutcome:
    """
    Attempt to initialize the service, classifying login failures and
    routing each failure type to the right retry strategy.

    Returns SUCCESS if initialization completed. Returns the halt-classified
    outcome (DEVICE_CHALLENGE, BAD_CREDENTIALS, RATE_LIMITED, TRANSIENT, UNKNOWN)
    if the caller should give up and exit cleanly.
    """
    transient_attempts = 0
    rate_limit_attempts = 0

    # Attempt once unconditionally — even if shutdown is already set we want
    # to try, otherwise tests that pre-set the shutdown flag (and any
    # already-in-flight SIGTERM landing during startup) would skip init entirely.
    while True:
        if service.initialize():
            return LoginOutcome.SUCCESS

        # initialize() returned False — was that login, or downstream infra?
        # If the Robinhood client never logged in, classify and route.
        # Otherwise (login succeeded but Kafka/Redis failed), treat as transient.
        client = service.robinhood
        outcome = (
            client.last_login_outcome
            if client is not None
            else LoginOutcome.UNKNOWN
        )
        if outcome == LoginOutcome.SUCCESS:
            # Login worked but a downstream connector failed — short retry.
            outcome = LoginOutcome.TRANSIENT

        if outcome.is_halt:
            _halt_with_alert(
                notifier,
                outcome,
                detail=(
                    "robin_stocks reported a device-approval verification "
                    "workflow on this attempt. The Pi cannot complete that "
                    "challenge headlessly."
                    if outcome == LoginOutcome.DEVICE_CHALLENGE
                    else "robin_stocks rejected the credentials without "
                         "issuing a device challenge."
                ),
            )
            return outcome

        if outcome == LoginOutcome.RATE_LIMITED:
            rate_limit_attempts += 1
            if rate_limit_attempts > _RATE_LIMIT_MAX_ATTEMPTS:
                _halt_with_alert(
                    notifier,
                    outcome,
                    detail=(
                        f"Robinhood has rate-limited login for "
                        f"{_RATE_LIMIT_MAX_ATTEMPTS * _RATE_LIMIT_COOLDOWN_SEC // 3600}+ hours. "
                        f"Refusing to keep retrying."
                    ),
                )
                return outcome
            wait = _RATE_LIMIT_COOLDOWN_SEC
            logger.warning(
                f"Rate-limited by Robinhood "
                f"({rate_limit_attempts}/{_RATE_LIMIT_MAX_ATTEMPTS}). "
                f"Sleeping {wait // 3600}h before next attempt."
            )
            _interruptible_sleep(wait)
            if _shutdown_requested:
                return LoginOutcome.UNKNOWN
            continue

        # TRANSIENT or UNKNOWN — exponential backoff with hard ceiling
        transient_attempts += 1
        if transient_attempts > _TRANSIENT_MAX_ATTEMPTS:
            _halt_with_alert(
                notifier,
                outcome,
                detail=(
                    f"Exceeded {_TRANSIENT_MAX_ATTEMPTS} transient login "
                    f"failures (outcome={outcome.value}). "
                    f"Refusing to keep retrying — likely something deeper is broken."
                ),
            )
            return outcome
        wait = min(
            _TRANSIENT_BACKOFF_BASE_SEC * (2 ** (transient_attempts - 1)),
            _TRANSIENT_BACKOFF_CAP_SEC,
        )
        logger.warning(
            f"Transient login/init failure "
            f"({transient_attempts}/{_TRANSIENT_MAX_ATTEMPTS}, "
            f"outcome={outcome.value}). Retrying in {wait // 60}m."
        )
        _interruptible_sleep(wait)

        if _shutdown_requested:
            return LoginOutcome.UNKNOWN


def run_continuous(settings: Settings, since_days: Optional[int] = None) -> int:
    """
    Run continuous sync with market-hours-aware scheduling.

    Syncs every poll_interval_minutes during market hours (Mon-Fri, 4am-8pm ET).
    Sleeps until next market open during off-hours and weekends.

    Args:
        settings: Application settings.
        since_days: Days of history to sync.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    global _shutdown_requested

    service = TradeSyncService(settings)
    scheduler = MarketScheduler(
        market_open_hour=settings.market_open_hour,
        market_close_hour=settings.market_close_hour,
        poll_interval_minutes=settings.poll_interval_minutes,
    )

    sync_days = since_days or settings.sync_history_days

    logger.info("Go Bears!!!!")

    notifier = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
    )

    try:
        init_outcome = _initialize_with_retries(service, notifier)
        if init_outcome != LoginOutcome.SUCCESS:
            # Halt cleanly. With docker-compose `restart: unless-stopped`,
            # exit-code 0 leaves the container stopped until a human acts.
            return 0

        # Log startup info
        logger.info("=" * 60)
        logger.info("Starting Continuous Sync Mode")
        logger.info("=" * 60)
        scheduler.log_status()
        logger.info(f"Poll interval: {settings.poll_interval_minutes} minutes")
        logger.info(f"Sync history: {sync_days} days")
        logger.info("=" * 60)

        # Always do an initial sync at startup
        logger.info("Performing initial sync at startup...")
        try:
            import time as _time
            _initial_start = _time.monotonic()
            new_synced, skipped = service.sync_trades(since_days=sync_days)
            logger.info(f"Initial sync complete: {new_synced} new trades, {skipped} skipped")
            # Also sync current positions
            service.sync_positions()
            logger.info("Current positions synced")
            # Sync watchlist
            added, removed = service.sync_watchlist()
            logger.info(f"Initial watchlist sync: {added} added, {removed} removed")
            # Sync stop orders
            stop_count = service.sync_stop_orders()
            logger.info(f"Initial stop orders sync: {stop_count} orders")
            # Sync earnings calendar on startup
            earnings_count = service.sync_earnings_calendar()
            _initial_duration = _time.monotonic() - _initial_start
            logger.info(
                f"Initial sync complete in {_initial_duration:.1f}s: "
                f"earnings={earnings_count} symbols"
            )
            SYNC_CYCLES.inc()
            CYCLE_DURATION.observe(_initial_duration)
            LAST_SUCCESS.set_to_current_time()
            CONSECUTIVE_FAILURES.set(0)
        except Exception as e:
            logger.error(f"Initial sync failed: {e}")
            CONSECUTIVE_FAILURES.set(1)
            # Continue anyway, will retry in the loop

        sync_count = 1
        consecutive_failures = 0
        max_consecutive_failures = 5

        # Main loop
        while not _shutdown_requested:
            # Calculate sleep duration based on market hours
            sleep_duration = scheduler.get_sleep_duration()
            sleep_seconds = sleep_duration.total_seconds()

            if scheduler.is_market_hours():
                logger.info(
                    f"Market is OPEN. Next sync in {settings.poll_interval_minutes} minutes..."
                )
            else:
                status = scheduler.get_status()
                hours_until_open = sleep_seconds / 3600
                logger.info(
                    f"Market is CLOSED ({status['day_of_week']}). "
                    f"Sleeping {hours_until_open:.1f} hours until {status.get('next_market_open', 'next open')}..."
                )

            # Sleep in small increments to allow for graceful shutdown
            sleep_increment = 30  # Check every 30 seconds
            slept = 0.0

            while slept < sleep_seconds and not _shutdown_requested:
                actual_sleep = min(sleep_increment, sleep_seconds - slept)
                time.sleep(actual_sleep)
                slept += actual_sleep

            if _shutdown_requested:
                break

            # Only sync during market hours
            if not scheduler.is_market_hours():
                logger.debug("Woke up outside market hours, recalculating sleep...")
                continue

            # Keep the access token ahead of its expiry. Cheap no-op unless the
            # token is past half its life; doing it here means the password
            # login path (the only one that can raise a device challenge) is
            # never reached in steady state.
            if not service.ensure_session():
                logger.warning("Session could not be refreshed — reconnecting")

            # Health check before sync
            if not service.is_healthy():
                logger.warning("Service unhealthy, attempting reconnect...")
                if not service.reconnect():
                    consecutive_failures += 1
                    CONSECUTIVE_FAILURES.set(consecutive_failures)
                    logger.error(
                        f"Reconnect failed ({consecutive_failures}/{max_consecutive_failures})"
                    )
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error("Too many consecutive failures, exiting...")
                        return 1
                    continue
                consecutive_failures = 0
                CONSECUTIVE_FAILURES.set(0)

            # Perform sync
            sync_count += 1
            logger.info(f"Starting sync #{sync_count}...")

            try:
                import time as _time
                _sync_start = _time.monotonic()
                # Use shorter history for incremental syncs
                incremental_days = min(sync_days, 7)
                # Also sync current positions
                service.sync_positions()
                logger.info("Current positions synced")
                new_synced, skipped = service.sync_trades(since_days=incremental_days)
                added, removed = service.sync_watchlist()
                if added or removed:
                    logger.info(f"Watchlist changes: {added} added, {removed} removed")
                # Sync stop orders
                stop_count = service.sync_stop_orders()
                _sync_duration = _time.monotonic() - _sync_start
                logger.info(
                    f"Sync #{sync_count} complete in {_sync_duration:.1f}s: "
                    f"{new_synced} new trades, {skipped} skipped, {stop_count} stop orders"
                )
                consecutive_failures = 0
                SYNC_CYCLES.inc()
                CYCLE_DURATION.observe(_sync_duration)
                LAST_SUCCESS.set_to_current_time()
                CONSECUTIVE_FAILURES.set(0)

                # Log stats and sync earnings periodically
                # (every hour = 6 syncs at 10 min intervals)
                if sync_count % 6 == 0:
                    stats = service.get_sync_stats()
                    logger.info(f"Sync stats: {stats}")
                    earnings_count = service.sync_earnings_calendar()
                    logger.info(f"Earnings calendar refresh: {earnings_count} symbols")

            except Exception as e:
                consecutive_failures += 1
                CONSECUTIVE_FAILURES.set(consecutive_failures)
                logger.error(
                    f"Sync #{sync_count} failed: {e} "
                    f"({consecutive_failures}/{max_consecutive_failures})"
                )
                if consecutive_failures >= max_consecutive_failures:
                    logger.error("Too many consecutive failures, exiting...")
                    return 1

        logger.info("Shutdown requested")
        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1

    finally:
        service.cleanup()
        logger.info("Service stopped")


def _start_health_server() -> None:
    """Start a minimal HTTP health server on a daemon thread."""
    port = int(os.environ.get("HEALTH_PORT", "8080"))

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args):
            pass  # suppress HTTP access logs

    server = HTTPServer(("", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Health server listening on :{port}/health")


def main():
    """Main entry point."""
    # Load environment variables from .env file
    load_dotenv()

    _start_health_server()
    start_metrics_server()

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Sync trades from Robinhood to Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run as continuous service (production mode)
  # Syncs during market hours Mon-Fri 4am-8pm ET
  python -m robinhood_sync.main

  # Sync once and exit
  python -m robinhood_sync.main --once

  # Sync only last 7 days of trades
  python -m robinhood_sync.main --once --days 7

  # Run with debug logging
  python -m robinhood_sync.main --debug
        """,
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run sync once and exit (default: continuous mode)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Days of trade history to sync (default: from config)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Configure debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("robin_stocks").setLevel(logging.DEBUG)

    # Load settings
    try:
        settings = get_settings()
    except Exception as e:
        logger.error(f"Failed to load settings: {e}")
        logger.error("Make sure ROBINHOOD_USERNAME and ROBINHOOD_PASSWORD are set")
        sys.exit(1)

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Log startup info
    logger.info("=" * 60)
    logger.info("Robinhood Sync Service")
    logger.info("Go Bears!!!!")
    logger.info("=" * 60)
    logger.info(f"Kafka brokers: {settings.kafka_brokers}")
    logger.info(f"Kafka topics: trades={settings.kafka_topic}, positions={settings.kafka_positions_topic}, watchlist={settings.kafka_watchlist_topic}")
    logger.info(f"Redis: {settings.redis_host}:{settings.redis_port}")
    logger.info(f"Mode: {'single sync' if args.once else 'continuous'}")
    if not args.once:
        logger.info(f"Market hours: {settings.market_open_hour}:00 - {settings.market_close_hour}:00 ET (Mon-Fri)")
        logger.info(f"Poll interval: {settings.poll_interval_minutes} minutes")
    logger.info("=" * 60)

    # Run the service
    if args.once:
        exit_code = run_once(settings, since_days=args.days)
    else:
        exit_code = run_continuous(settings, since_days=args.days)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
