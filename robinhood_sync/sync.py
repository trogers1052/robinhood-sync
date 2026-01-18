"""
Main sync logic for Robinhood trade synchronization.
"""

import logging
from typing import Optional

from .config import Settings
from .robinhood_client import RobinhoodClient, Trade
from .kafka_producer import TradeEventProducer
from .redis_client import SyncedOrdersTracker

logger = logging.getLogger(__name__)


class TradeSyncService:
    """
    Service that syncs trades from Robinhood to Kafka.

    Tracks which orders have already been synced using Redis to avoid duplicates.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.robinhood: Optional[RobinhoodClient] = None
        self.kafka: Optional[TradeEventProducer] = None
        self.tracker: Optional[SyncedOrdersTracker] = None

    def initialize(self) -> bool:
        """
        Initialize all connections.

        Returns:
            True if all connections successful, False otherwise.
        """
        # Initialize Robinhood client
        self.robinhood = RobinhoodClient(
            username=self.settings.robinhood_username,
            password=self.settings.robinhood_password,
            totp_secret=self.settings.robinhood_totp_secret,
        )

        if not self.robinhood.login():
            logger.error("Failed to login to Robinhood")
            return False

        # Initialize Kafka producer
        self.kafka = TradeEventProducer(
            brokers=self.settings.kafka_broker_list,
            topic=self.settings.kafka_topic,
        )

        if not self.kafka.connect():
            logger.error("Failed to connect to Kafka")
            return False

        # Initialize Redis tracker
        self.tracker = SyncedOrdersTracker(self.settings)

        if not self.tracker.connect():
            logger.error("Failed to connect to Redis")
            return False

        logger.info("All connections initialized successfully")
        return True

    def sync_trades(self, since_days: Optional[int] = None) -> tuple[int, int]:
        """
        Sync filled trades from Robinhood.

        Args:
            since_days: Only sync trades from the last N days. None for all.

        Returns:
            Tuple of (new_trades_synced, skipped_already_synced).
        """
        if not self.robinhood or not self.kafka or not self.tracker:
            raise RuntimeError("Service not initialized")

        logger.info("Starting trade sync...")

        # Get filled orders from Robinhood
        filled_trades = self.robinhood.get_filled_orders(since_days=since_days)

        if not filled_trades:
            logger.info("No filled trades found")
            return 0, 0

        # Filter out already synced orders
        new_trades = []
        skipped = 0

        for trade in filled_trades:
            if self.tracker.is_synced(trade.order_id):
                skipped += 1
            else:
                new_trades.append(trade)

        if skipped > 0:
            logger.info(f"Skipping {skipped} already synced trades")

        if not new_trades:
            logger.info("No new trades to sync")
            return 0, skipped

        logger.info(f"Found {len(new_trades)} new trades to sync")

        # Publish new trades to Kafka
        synced = 0
        for trade in new_trades:
            if self.kafka.publish_trade(trade):
                self.tracker.mark_synced(trade.order_id)
                synced += 1
                logger.info(
                    f"Synced trade: {trade.side.upper()} {trade.quantity} {trade.symbol} "
                    f"@ ${trade.average_price}"
                )

        logger.info(f"Sync complete: {synced} new trades synced, {skipped} skipped")
        return synced, skipped

    def get_sync_stats(self) -> dict:
        """
        Get statistics about synced trades.

        Returns:
            Dict with sync statistics.
        """
        if not self.tracker:
            raise RuntimeError("Service not initialized")

        return {
            "total_synced_orders": self.tracker.count_synced(),
        }

    def cleanup(self) -> None:
        """Clean up connections."""
        if self.robinhood:
            self.robinhood.logout()

        if self.kafka:
            self.kafka.close()

        if self.tracker:
            self.tracker.close()

        logger.info("All connections closed")

    def is_healthy(self) -> bool:
        """
        Check if the service is healthy and connected.

        Returns:
            True if all connections are healthy.
        """
        try:
            # Check Robinhood
            if not self.robinhood or not self.robinhood.is_logged_in():
                logger.warning("Robinhood not logged in")
                return False

            # Check Redis
            if not self.tracker:
                logger.warning("Redis tracker not initialized")
                return False

            # Try a simple Redis operation
            self.tracker.count_synced()

            return True

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def reconnect(self) -> bool:
        """
        Attempt to reconnect all services.

        Returns:
            True if reconnection successful.
        """
        logger.info("Attempting to reconnect...")

        # Cleanup existing connections
        self.cleanup()

        # Re-initialize
        return self.initialize()
