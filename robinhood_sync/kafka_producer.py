"""
Kafka producer for publishing trade events.
"""

import json
import logging
from datetime import datetime
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .robinhood_client import Trade, Position, AccountBalance, WatchlistStock

logger = logging.getLogger(__name__)


class TradeEventProducer:
    """Produces trade, position, and watchlist events to Kafka."""

    def __init__(
        self,
        brokers: list[str],
        topic: str,
        positions_topic: Optional[str] = None,
        watchlist_topic: Optional[str] = None,
    ):
        """
        Initialize the Kafka producer.

        Args:
            brokers: List of Kafka broker addresses.
            topic: Topic to publish trade events to.
            positions_topic: Topic to publish position snapshots to.
            watchlist_topic: Topic to publish watchlist events to.
        """
        self.brokers = brokers
        self.topic = topic
        self.positions_topic = positions_topic or "trading.positions"
        self.watchlist_topic = watchlist_topic or "trading.watchlist"
        self._producer: Optional[KafkaProducer] = None

    def connect(self) -> bool:
        """
        Connect to Kafka brokers.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to Kafka brokers: {self.brokers}")

            self._producer = KafkaProducer(
                bootstrap_servers=self.brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Wait for all replicas to acknowledge
                retries=3,
                retry_backoff_ms=1000,
            )

            logger.info("Successfully connected to Kafka")
            return True

        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def close(self) -> None:
        """Close the Kafka producer connection."""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")

    def publish_trade(self, trade: Trade) -> bool:
        """
        Publish a single trade event to Kafka.

        Args:
            trade: Trade object to publish.

        Returns:
            True if published successfully, False otherwise.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")

        try:
            # Create the event payload
            event = {
                "event_type": "TRADE_DETECTED",
                "source": "robinhood",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "data": trade.to_dict(),
            }

            # Use symbol as the key for partitioning
            key = trade.symbol

            # Log the message being sent
            logger.info(f"Publishing trade to Kafka topic '{self.topic}':")
            logger.info(f"  Key: {key}")
            logger.info(f"  Event: {json.dumps(event, indent=2)}")

            # Send the message
            future = self._producer.send(
                self.topic,
                key=key,
                value=event,
            )

            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Successfully published trade {trade.order_id} to "
                f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish trade {trade.order_id}: {e}")
            return False

    def publish_trades(self, trades: list[Trade]) -> tuple[int, int]:
        """
        Publish multiple trades to Kafka.

        Args:
            trades: List of Trade objects to publish.

        Returns:
            Tuple of (successful_count, failed_count).
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")

        successful = 0
        failed = 0

        for trade in trades:
            if self.publish_trade(trade):
                successful += 1
            else:
                failed += 1

        # Flush to ensure all messages are sent
        self._producer.flush()

        logger.info(f"Published {successful} trades, {failed} failed")
        return successful, failed

    def publish_positions(
        self, positions: list[Position], balance: AccountBalance
    ) -> bool:
        """
        Publish a positions snapshot to Kafka.

        Args:
            positions: List of current Position objects.
            balance: AccountBalance object with buying power info.

        Returns:
            True if published successfully, False otherwise.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")

        try:
            # Create the event payload
            event = {
                "event_type": "POSITIONS_SNAPSHOT",
                "source": "robinhood",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "data": {
                    "positions": [p.to_dict() for p in positions],
                    "buying_power": str(balance.buying_power),
                    "cash": str(balance.cash),
                    "total_equity": str(balance.total_equity),
                },
            }

            # Use "positions" as the key
            key = "positions"

            logger.info(f"Publishing positions snapshot to Kafka topic '{self.positions_topic}':")
            logger.info(f"  Positions: {len(positions)}")
            logger.info(f"  Buying power: ${balance.buying_power}")

            # Send the message
            future = self._producer.send(
                self.positions_topic,
                key=key,
                value=event,
            )

            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Successfully published positions snapshot to "
                f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish positions snapshot: {e}")
            return False

    def publish_watchlist_update(
        self,
        added_symbols: list[str],
        removed_symbols: list[str],
        all_symbols: list[str],
        stocks: Optional[list[WatchlistStock]] = None,
    ) -> bool:
        """
        Publish a watchlist update event to Kafka.

        This event notifies other services when the watchlist changes.

        Args:
            added_symbols: List of newly added symbols.
            removed_symbols: List of removed symbols.
            all_symbols: Complete list of all symbols in watchlist.
            stocks: Optional list of WatchlistStock objects with full details.

        Returns:
            True if published successfully, False otherwise.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")

        try:
            # Create the event payload
            event = {
                "event_type": "WATCHLIST_UPDATED",
                "source": "robinhood",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "data": {
                    "added_symbols": added_symbols,
                    "removed_symbols": removed_symbols,
                    "all_symbols": all_symbols,
                    "total_count": len(all_symbols),
                    "stocks": [s.to_dict() for s in stocks] if stocks else [],
                },
            }

            key = "watchlist"

            logger.info(f"Publishing watchlist update to Kafka topic '{self.watchlist_topic}':")
            logger.info(f"  Added: {added_symbols}")
            logger.info(f"  Removed: {removed_symbols}")
            logger.info(f"  Total symbols: {len(all_symbols)}")

            # Send the message
            future = self._producer.send(
                self.watchlist_topic,
                key=key,
                value=event,
            )

            # Wait for send to complete
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Successfully published watchlist update to "
                f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish watchlist update: {e}")
            return False

    def publish_symbol_added(self, symbol: str, name: str = "") -> bool:
        """
        Publish an event when a single symbol is added to the watchlist.

        Args:
            symbol: Stock symbol that was added.
            name: Optional company name.

        Returns:
            True if published successfully, False otherwise.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")

        try:
            event = {
                "event_type": "WATCHLIST_SYMBOL_ADDED",
                "source": "robinhood",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "data": {
                    "symbol": symbol,
                    "name": name or symbol,
                },
            }

            # Use the symbol as the key
            key = symbol

            logger.info(f"Publishing symbol added event: {symbol}")

            future = self._producer.send(
                self.watchlist_topic,
                key=key,
                value=event,
            )

            record_metadata = future.get(timeout=10)

            logger.info(
                f"Successfully published symbol added event to "
                f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish symbol added event: {e}")
            return False

    def publish_symbol_removed(self, symbol: str) -> bool:
        """
        Publish an event when a symbol is removed from the watchlist.

        Args:
            symbol: Stock symbol that was removed.

        Returns:
            True if published successfully, False otherwise.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")

        try:
            event = {
                "event_type": "WATCHLIST_SYMBOL_REMOVED",
                "source": "robinhood",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "data": {
                    "symbol": symbol,
                },
            }

            key = symbol

            logger.info(f"Publishing symbol removed event: {symbol}")

            future = self._producer.send(
                self.watchlist_topic,
                key=key,
                value=event,
            )

            record_metadata = future.get(timeout=10)

            logger.info(
                f"Successfully published symbol removed event to "
                f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish symbol removed event: {e}")
            return False
