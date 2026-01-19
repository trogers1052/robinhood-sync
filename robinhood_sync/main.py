"""
Robinhood Sync Service - Main Entry Point

Syncs trades from Robinhood to Kafka for processing by other services.
Runs continuously during market hours (Mon-Fri, 4am-8pm ET).
"""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

from .config import get_settings, Settings
from .sync import TradeSyncService
from .scheduler import MarketScheduler

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

        new_synced, skipped = service.sync_trades(since_days=since_days)
        # Also sync current positions
        service.sync_positions()
        # Sync watchlist
        added, removed = service.sync_watchlist()
        logger.info(f"Sync complete: {new_synced} new trades, {skipped} already synced")
        logger.info(f"Watchlist: {added} added, {removed} removed")
        return 0

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return 1

    finally:
        service.cleanup()


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

    try:
        if not service.initialize():
            logger.error("Failed to initialize service")
            return 1

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
            new_synced, skipped = service.sync_trades(since_days=sync_days)
            logger.info(f"Initial sync complete: {new_synced} new trades, {skipped} skipped")
            # Also sync current positions
            service.sync_positions()
            logger.info("Current positions synced")
            # Sync watchlist
            added, removed = service.sync_watchlist()
            logger.info(f"Initial watchlist sync: {added} added, {removed} removed")
        except Exception as e:
            logger.error(f"Initial sync failed: {e}")
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

            # Health check before sync
            if not service.is_healthy():
                logger.warning("Service unhealthy, attempting reconnect...")
                if not service.reconnect():
                    consecutive_failures += 1
                    logger.error(
                        f"Reconnect failed ({consecutive_failures}/{max_consecutive_failures})"
                    )
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error("Too many consecutive failures, exiting...")
                        return 1
                    continue
                consecutive_failures = 0

            # Perform sync
            sync_count += 1
            logger.info(f"Starting sync #{sync_count}...")

            try:
                # Use shorter history for incremental syncs
                incremental_days = min(sync_days, 7)
                # Also sync current positions
                service.sync_positions()
                logger.info("Current positions synced")
                new_synced, skipped = service.sync_trades(since_days=incremental_days)
                added, removed = service.sync_watchlist()
                if added or removed:
                    logger.info(f"Watchlist changes: {added} added, {removed} removed")
                new_synced, skipped = service.sync_trades(since_days=incremental_days)
                logger.info(
                    f"Sync #{sync_count} complete: {new_synced} new trades, {skipped} skipped"
                )
                consecutive_failures = 0

                # Log stats periodically (every hour = 6 syncs at 10 min intervals)
                if sync_count % 6 == 0:
                    stats = service.get_sync_stats()
                    logger.info(f"Sync stats: {stats}")

            except Exception as e:
                consecutive_failures += 1
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


def main():
    """Main entry point."""
    # Load environment variables from .env file
    load_dotenv()

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
