"""
Redis client for tracking synced order IDs and storing positions.
"""

import json
import logging
from typing import Optional, Set, TYPE_CHECKING

import redis

from .config import Settings

if TYPE_CHECKING:
    from .robinhood_client import Position, AccountBalance

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


class PositionStore:
    """Stores current positions and account balance in Redis."""

    POSITIONS_KEY = "robinhood:positions"
    BUYING_POWER_KEY = "robinhood:buying_power"
    POSITIONS_TTL = 3600  # 1 hour TTL for stale position cleanup

    def __init__(self, settings: Settings):
        """
        Initialize the Redis client for position storage.

        Args:
            settings: Application settings containing Redis configuration.
        """
        self.settings = settings
        self._client: Optional[redis.Redis] = None

    def connect(self) -> bool:
        """
        Connect to Redis.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"PositionStore connecting to Redis at {self.settings.redis_host}:{self.settings.redis_port}")

            self._client = redis.Redis(
                host=self.settings.redis_host,
                port=self.settings.redis_port,
                password=self.settings.redis_password,
                db=self.settings.redis_db,
                decode_responses=True,
            )

            # Test connection
            self._client.ping()
            logger.info("PositionStore connected to Redis")
            return True

        except redis.RedisError as e:
            logger.error(f"PositionStore failed to connect to Redis: {e}")
            return False

    def close(self) -> None:
        """Close the Redis connection."""
        if self._client:
            self._client.close()
            logger.info("PositionStore Redis connection closed")

    def store_positions(self, positions: list["Position"]) -> bool:
        """
        Store current positions in Redis.

        Replaces all existing positions with the new snapshot.

        Args:
            positions: List of Position objects to store.

        Returns:
            True if successful, False otherwise.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            # Delete existing positions
            self._client.delete(self.POSITIONS_KEY)

            if not positions:
                logger.info("No positions to store in Redis")
                return True

            # Store each position as a hash field
            position_data = {p.symbol: json.dumps(p.to_dict()) for p in positions}
            self._client.hset(self.POSITIONS_KEY, mapping=position_data)

            # Set TTL for automatic cleanup
            self._client.expire(self.POSITIONS_KEY, self.POSITIONS_TTL)

            logger.info(f"Stored {len(positions)} positions in Redis")
            return True

        except redis.RedisError as e:
            logger.error(f"Failed to store positions in Redis: {e}")
            return False

    def store_buying_power(self, balance: "AccountBalance") -> bool:
        """
        Store account balance/buying power in Redis.

        Args:
            balance: AccountBalance object to store.

        Returns:
            True if successful, False otherwise.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            self._client.set(
                self.BUYING_POWER_KEY,
                json.dumps(balance.to_dict()),
                ex=self.POSITIONS_TTL,
            )
            logger.info(f"Stored buying power in Redis: ${balance.buying_power}")
            return True

        except redis.RedisError as e:
            logger.error(f"Failed to store buying power in Redis: {e}")
            return False

    def get_positions(self) -> dict:
        """
        Get all stored positions from Redis.

        Returns:
            Dictionary mapping symbol to position data.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            data = self._client.hgetall(self.POSITIONS_KEY)
            return {symbol: json.loads(pos_json) for symbol, pos_json in data.items()}
        except redis.RedisError as e:
            logger.error(f"Failed to get positions from Redis: {e}")
            return {}

    def get_buying_power(self) -> Optional[dict]:
        """
        Get stored buying power from Redis.

        Returns:
            AccountBalance data as dict, or None if not found.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            data = self._client.get(self.BUYING_POWER_KEY)
            if data:
                return json.loads(data)
            return None
        except redis.RedisError as e:
            logger.error(f"Failed to get buying power from Redis: {e}")
            return None
