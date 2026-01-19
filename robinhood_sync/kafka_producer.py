"""
Kafka producer for publishing trade events.
"""

import json
import logging
from datetime import datetime
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .robinhood_client import Trade, Position, AccountBalance

logger = logging.getLogger(__name__)


class TradeEventProducer:
    """Produces trade and position events to Kafka."""

    def __init__(self, brokers: list[str], topic: str, positions_topic: Optional[str] = None):
        """
        Initialize the Kafka producer.

        Args:
            brokers: List of Kafka broker addresses.
            topic: Topic to publish trade events to.
            positions_topic: Topic to publish position snapshots to.
        """
        self.brokers = brokers
        self.topic = topic
        self.positions_topic = positions_topic or "trading.positions"
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
