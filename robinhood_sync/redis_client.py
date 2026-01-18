"""
Redis client for tracking synced order IDs.
"""

import logging
from typing import Optional, Set

import redis

from .config import Settings

logger = logging.getLogger(__name__)


class SyncedOrdersTracker:
    """Tracks which order IDs have been synced using Redis."""

    def __init__(self, settings: Settings):
        """
        Initialize the Redis client.

        Args:
            settings: Application settings containing Redis configuration.
        """
        self.settings = settings
        self.key = settings.redis_synced_orders_key
        self._client: Optional[redis.Redis] = None

    def connect(self) -> bool:
        """
        Connect to Redis.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to Redis at {self.settings.redis_host}:{self.settings.redis_port}")

            self._client = redis.Redis(
                host=self.settings.redis_host,
                port=self.settings.redis_port,
                password=self.settings.redis_password,
                db=self.settings.redis_db,
                decode_responses=True,
            )

            # Test connection
            self._client.ping()

            count = self._client.scard(self.key)
            logger.info(f"Connected to Redis. Tracking {count} synced orders.")
            return True

        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False

    def close(self) -> None:
        """Close the Redis connection."""
        if self._client:
            self._client.close()
            logger.info("Redis connection closed")

    def is_synced(self, order_id: str) -> bool:
        """
        Check if an order has already been synced.

        Args:
            order_id: The order ID to check.

        Returns:
            True if already synced, False otherwise.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        return self._client.sismember(self.key, order_id)

    def mark_synced(self, order_id: str) -> None:
        """
        Mark an order as synced.

        Args:
            order_id: The order ID to mark as synced.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        self._client.sadd(self.key, order_id)

    def mark_many_synced(self, order_ids: list[str]) -> None:
        """
        Mark multiple orders as synced.

        Args:
            order_ids: List of order IDs to mark as synced.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        if order_ids:
            self._client.sadd(self.key, *order_ids)

    def get_all_synced(self) -> Set[str]:
        """
        Get all synced order IDs.

        Returns:
            Set of synced order IDs.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        return self._client.smembers(self.key)

    def count_synced(self) -> int:
        """
        Get the count of synced orders.

        Returns:
            Number of synced orders.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        return self._client.scard(self.key)

    def remove_synced(self, order_id: str) -> None:
        """
        Remove an order from the synced set (for re-processing).

        Args:
            order_id: The order ID to remove.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        self._client.srem(self.key, order_id)

    def clear_all(self) -> None:
        """Clear all synced order IDs (use with caution)."""
        if not self._client:
            raise RuntimeError("Redis client not connected")

        self._client.delete(self.key)
        logger.warning("Cleared all synced order IDs from Redis")
