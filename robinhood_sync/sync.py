"""
Main sync logic for Robinhood trade synchronization.
"""

import logging
from typing import Optional, Set
import psycopg2
from psycopg2.extras import RealDictCursor

from .config import Settings
from .robinhood_client import RobinhoodClient, Trade
from .kafka_producer import TradeEventProducer

logger = logging.getLogger(__name__)


class TradeSyncService:
    """
    Service that syncs trades from Robinhood to Kafka.

    Tracks which orders have already been synced to avoid duplicates.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.robinhood: Optional[RobinhoodClient] = None
        self.kafka: Optional[TradeEventProducer] = None
        self._db_conn = None
        self._synced_order_ids: Set[str] = set()

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

        # Initialize database connection for tracking synced orders
        try:
            self._connect_database()
            self._ensure_sync_table()
            self._load_synced_order_ids()
        except Exception as e:
            logger.warning(f"Database connection failed: {e}. Will not track synced orders.")

        return True

    def _connect_database(self) -> None:
        """Connect to PostgreSQL database."""
        self._db_conn = psycopg2.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            user=self.settings.db_user,
            password=self.settings.db_password,
            database=self.settings.db_name,
        )
        logger.info("Connected to PostgreSQL database")

    def _ensure_sync_table(self) -> None:
        """Create the sync tracking table if it doesn't exist."""
        if not self._db_conn:
            return

        with self._db_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS robinhood_synced_orders (
                    order_id VARCHAR(255) PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self._db_conn.commit()
        logger.info("Ensured robinhood_synced_orders table exists")

    def _load_synced_order_ids(self) -> None:
        """Load already synced order IDs from database."""
        if not self._db_conn:
            return

        with self._db_conn.cursor() as cur:
            cur.execute("SELECT order_id FROM robinhood_synced_orders")
            rows = cur.fetchall()
            self._synced_order_ids = {row[0] for row in rows}

        logger.info(f"Loaded {len(self._synced_order_ids)} previously synced order IDs")

    def _mark_order_synced(self, trade: Trade) -> None:
        """Mark an order as synced in the database."""
        if not self._db_conn:
            return

        try:
            with self._db_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO robinhood_synced_orders (order_id, symbol, side)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING
                    """,
                    (trade.order_id, trade.symbol, trade.side),
                )
                self._db_conn.commit()
            self._synced_order_ids.add(trade.order_id)
        except Exception as e:
            logger.warning(f"Failed to mark order {trade.order_id} as synced: {e}")

    def sync_all_trades(self, since_days: Optional[int] = None) -> tuple[int, int]:
        """
        Sync all filled trades from Robinhood.

        Args:
            since_days: Only sync trades from the last N days. None for all.

        Returns:
            Tuple of (new_trades_synced, skipped_already_synced).
        """
        if not self.robinhood or not self.kafka:
            raise RuntimeError("Service not initialized")

        logger.info("Starting trade sync...")

        # Get filled orders from Robinhood
        filled_trades = self.robinhood.get_filled_orders(since_days=since_days)

        if not filled_trades:
            logger.info("No filled trades found")
            return 0, 0

        # Filter out already synced orders
        new_trades = [
            t for t in filled_trades
            if t.order_id not in self._synced_order_ids
        ]
        skipped = len(filled_trades) - len(new_trades)

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
                self._mark_order_synced(trade)
                synced += 1
                logger.info(
                    f"Synced trade: {trade.side.upper()} {trade.quantity} {trade.symbol} "
                    f"@ ${trade.average_price}"
                )

        logger.info(f"Sync complete: {synced} new trades synced, {skipped} skipped")
        return synced, skipped

    def cleanup(self) -> None:
        """Clean up connections."""
        if self.robinhood:
            self.robinhood.logout()

        if self.kafka:
            self.kafka.close()

        if self._db_conn:
            self._db_conn.close()
            logger.info("Database connection closed")
