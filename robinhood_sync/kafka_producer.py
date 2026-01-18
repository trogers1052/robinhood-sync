"""
Kafka producer for publishing trade events.
"""

import json
import logging
from datetime import datetime
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .robinhood_client import Trade

logger = logging.getLogger(__name__)


class TradeEventProducer:
    """Produces trade events to Kafka."""

    def __init__(self, brokers: list[str], topic: str):
        """
        Initialize the Kafka producer.

        Args:
            brokers: List of Kafka broker addresses.
            topic: Topic to publish trade events to.
        """
        self.brokers = brokers
        self.topic = topic
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
