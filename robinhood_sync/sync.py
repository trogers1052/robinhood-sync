"""
Main sync logic for Robinhood trade synchronization.
"""

import logging
import robin_stocks.robinhood as rh
from typing import Optional

from .config import Settings
from .robinhood_client import RobinhoodClient, Trade
from .kafka_producer import TradeEventProducer
from .redis_client import SyncedOrdersTracker, PositionStore, WatchlistStore, StopOrderStore

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
        self.position_store: Optional[PositionStore] = None
        self.watchlist_store: Optional[WatchlistStore] = None
        self.stop_order_store: Optional[StopOrderStore] = None

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
            positions_topic=self.settings.kafka_positions_topic,
            watchlist_topic=self.settings.kafka_watchlist_topic,
        )

        if not self.kafka.connect():
            logger.error("Failed to connect to Kafka")
            return False

        # Initialize Redis tracker
        self.tracker = SyncedOrdersTracker(self.settings)

        if not self.tracker.connect():
            logger.error("Failed to connect to Redis")
            return False

        # Initialize Redis position store
        self.position_store = PositionStore(self.settings)

        if not self.position_store.connect():
            logger.error("Failed to connect to Redis for position store")
            return False

        # Initialize Redis watchlist store
        self.watchlist_store = WatchlistStore(self.settings)

        if not self.watchlist_store.connect():
            logger.error("Failed to connect to Redis for watchlist store")
            return False

        # Initialize Redis stop order store
        self.stop_order_store = StopOrderStore(self.settings)

        if not self.stop_order_store.connect():
            logger.error("Failed to connect to Redis for stop order store")
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

    def sync_positions(self) -> bool:
        """
        Sync current positions and buying power from Robinhood.

        Fetches positions and account balance, stores in Redis, and publishes to Kafka.

        Returns:
            True if sync successful, False otherwise.
        """
        if not self.robinhood or not self.kafka or not self.position_store:
            raise RuntimeError("Service not initialized")

        try:
            logger.info("Starting positions sync...")

            # Fetch current positions from Robinhood
            positions = self.robinhood.get_current_positions()

            # Fetch account balance
            balance = self.robinhood.get_account_balance()

            # Store in Redis
            self.position_store.store_positions(positions)
            self.position_store.store_buying_power(balance)

            # Publish to Kafka
            if self.kafka.publish_positions(positions, balance):
                logger.info(
                    f"Positions sync complete: {len(positions)} positions, "
                    f"buying power ${balance.buying_power}"
                )
                return True
            else:
                logger.error("Failed to publish positions to Kafka")
                return False

        except Exception as e:
            logger.error(f"Error syncing positions: {e}")
            return False

    def sync_stop_orders(self) -> int:
        """
        Sync pending stop loss orders from Robinhood to Redis.

        This allows stop-loss-guardian to know which positions have stop losses.

        Returns:
            Number of stop orders synced.
        """
        if not self.robinhood or not self.stop_order_store:
            raise RuntimeError("Service not initialized")

        try:
            logger.info("Starting stop orders sync...")

            # Fetch pending stop orders from Robinhood
            stop_orders = self.robinhood.get_stop_orders()

            # Store in Redis
            self.stop_order_store.store_stop_orders(stop_orders)

            if stop_orders:
                for order in stop_orders:
                    logger.info(
                        f"Stop order: {order.symbol} @ ${order.stop_price} "
                        f"(qty: {order.quantity}, state: {order.state})"
                    )

            logger.info(f"Stop orders sync complete: {len(stop_orders)} orders")
            return len(stop_orders)

        except Exception as e:
            logger.error(f"Error syncing stop orders: {e}")
            return 0

    def sync_watchlist(self) -> tuple[int, int]:
        """
        Sync watchlist from Robinhood.

        Fetches watchlist, syncs to Redis, and publishes Kafka events for changes.

        Returns:
            Tuple of (added_count, removed_count).
        """
        if not self.robinhood or not self.kafka or not self.watchlist_store:
            raise RuntimeError("Service not initialized")

        try:
            logger.info("Starting watchlist sync...")

            # Fetch watchlist from Robinhood
            watchlist_name = "Resources and assets"
            stocks = rh.account.get_watchlist_by_name(name=watchlist_name)['results']

            if not stocks:
                logger.info("No stocks found in Robinhood watchlist")
                # Still sync empty list to handle removals
                stocks = []

            # Sync to Redis and get changes
            added_symbols, removed_symbols = self.watchlist_store.sync_watchlist(stocks)
            # Publish Kafka events for changes
            if added_symbols or removed_symbols:
                # Publish overall watchlist update event
                logger.info(added_symbols)
                all_symbols = []
                for stock in stocks:
                    all_symbols.append(stock.get('symbol'))
                all_symbols = sorted(all_symbols)
                logger.info(all_symbols)
                self.kafka.publish_watchlist_update(
                    added_symbols=added_symbols,
                    removed_symbols=removed_symbols,
                    all_symbols=all_symbols,
                    stocks=stocks,
                )

                # Publish individual symbol events for granular updates
                for symbol in added_symbols:
                    # Find the stock details
                    for s in stocks:
                        logger.info(s)
                        if s.get('symbol') == symbol:
                            name = s.get('name') if s else symbol
                            self.kafka.publish_symbol_added(symbol, name)
                            logger.info(f"Published symbol added event: {symbol}")

                for symbol in removed_symbols:
                    self.kafka.publish_symbol_removed(symbol)
                    logger.info(f"Published symbol removed event: {symbol}")

            logger.info(
                f"Watchlist sync complete: {len(stocks)} total symbols, "
                f"{len(added_symbols)} added, {len(removed_symbols)} removed"
            )

            return len(added_symbols), len(removed_symbols)

        except Exception as e:
            logger.error(f"Error syncing watchlist: {e}")
            raise

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

        if self.position_store:
            self.position_store.close()

        if self.watchlist_store:
            self.watchlist_store.close()

        if self.stop_order_store:
            self.stop_order_store.close()

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
