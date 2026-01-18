"""
Robinhood Sync Service - Main Entry Point

Syncs trades from Robinhood to Kafka for processing by other services.
"""

import argparse
import logging
import signal
import sys
import time
from typing import Optional

from dotenv import load_dotenv

from .config import get_settings, Settings
from .sync import TradeSyncService

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


def run_once(settings: Settings, since_days: Optional[int] = None) -> None:
    """
    Run a single sync operation.

    Args:
        settings: Application settings.
        since_days: Only sync trades from the last N days.
    """
    service = TradeSyncService(settings)

    try:
        if not service.initialize():
            logger.error("Failed to initialize service")
            sys.exit(1)

        new_synced, skipped = service.sync_all_trades(since_days=since_days)
        logger.info(f"Sync complete: {new_synced} new trades, {skipped} already synced")

    finally:
        service.cleanup()


def run_continuous(settings: Settings) -> None:
    """
    Run continuous sync with polling.

    Args:
        settings: Application settings.
    """
    global _shutdown_requested

    service = TradeSyncService(settings)

    try:
        if not service.initialize():
            logger.error("Failed to initialize service")
            sys.exit(1)

        logger.info(
            f"Starting continuous sync (polling every {settings.poll_interval_seconds}s)..."
        )

        # Initial sync - get recent history
        logger.info(f"Initial sync: fetching last {settings.sync_history_days} days...")
        service.sync_all_trades(since_days=settings.sync_history_days)

        # Continuous polling
        while not _shutdown_requested:
            try:
                time.sleep(settings.poll_interval_seconds)

                if _shutdown_requested:
                    break

                logger.info("Polling for new trades...")
                # Only look at recent trades for incremental sync
                service.sync_all_trades(since_days=1)

            except Exception as e:
                logger.error(f"Error during sync: {e}")
                # Continue running, will retry next poll

    finally:
        service.cleanup()


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
  # Sync all trades once and exit
  python -m robinhood_sync.main --once

  # Sync only trades from last 7 days
  python -m robinhood_sync.main --once --days 7

  # Run continuously with polling
  python -m robinhood_sync.main

  # Run with custom poll interval (60 seconds)
  python -m robinhood_sync.main --interval 60
        """,
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run sync once and exit (default: continuous polling)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Only sync trades from the last N days (default: all)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Poll interval in seconds (default: from config)",
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

    # Override poll interval if specified
    if args.interval:
        settings.poll_interval_seconds = args.interval

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("=" * 60)
    logger.info("Robinhood Sync Service")
    logger.info("=" * 60)
    logger.info(f"Kafka brokers: {settings.kafka_brokers}")
    logger.info(f"Kafka topic: {settings.kafka_topic}")
    logger.info(f"Mode: {'once' if args.once else 'continuous'}")
    logger.info("=" * 60)

    # Run the service
    if args.once:
        run_once(settings, since_days=args.days)
    else:
        run_continuous(settings)

    logger.info("Service stopped")


if __name__ == "__main__":
    main()
