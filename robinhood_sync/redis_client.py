"""
Redis client for tracking synced order IDs, storing positions, and managing watchlist.
"""

import json
import logging
from typing import Optional, Set, TYPE_CHECKING

import redis

from .config import Settings

if TYPE_CHECKING:
    from .robinhood_client import Position, AccountBalance, WatchlistStock

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


class WatchlistStore:
    """
    Stores and manages watchlist symbols in Redis.

    This is the central source of truth for which symbols should be tracked
    across all services (market-data-ingestion, stock-service, etc.).
    """

    # Redis keys
    WATCHLIST_KEY = "trading:watchlist"           # Set of symbols
    WATCHLIST_DETAILS_KEY = "trading:watchlist:details"  # Hash of symbol -> details JSON

    def __init__(self, settings: Settings):
        """
        Initialize the Redis client for watchlist storage.

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
            logger.info(f"WatchlistStore connecting to Redis at {self.settings.redis_host}:{self.settings.redis_port}")

            self._client = redis.Redis(
                host=self.settings.redis_host,
                port=self.settings.redis_port,
                password=self.settings.redis_password,
                db=self.settings.redis_db,
                decode_responses=True,
            )

            # Test connection
            self._client.ping()

            count = self._client.scard(self.WATCHLIST_KEY)
            logger.info(f"WatchlistStore connected to Redis. {count} symbols in watchlist.")
            return True

        except redis.RedisError as e:
            logger.error(f"WatchlistStore failed to connect to Redis: {e}")
            return False

    def close(self) -> None:
        """Close the Redis connection."""
        if self._client:
            self._client.close()
            logger.info("WatchlistStore Redis connection closed")

    def sync_watchlist(self, stocks: list["WatchlistStock"]) -> tuple[list[str], list[str]]:
        """
        Sync the watchlist with Redis and return new/removed symbols.

        This is the main method that should be called when syncing from Robinhood.
        It compares the current Redis watchlist with the new list and identifies changes.

        Args:
            stocks: List of WatchlistStock objects from Robinhood.

        Returns:
            Tuple of (newly_added_symbols, removed_symbols)
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            # Get current watchlist from Redis
            current_symbols = self._client.smembers(self.WATCHLIST_KEY)

            # Get new symbols from Robinhood
            new_symbols = {stock.symbol for stock in stocks}

            # Calculate differences
            added_symbols = new_symbols - current_symbols
            removed_symbols = current_symbols - new_symbols

            # Update Redis with new watchlist
            if added_symbols or removed_symbols:
                # Use a pipeline for atomic updates
                pipe = self._client.pipeline()

                # Add new symbols
                if added_symbols:
                    pipe.sadd(self.WATCHLIST_KEY, *added_symbols)

                # Remove old symbols
                if removed_symbols:
                    pipe.srem(self.WATCHLIST_KEY, *removed_symbols)
                    # Also remove details for removed symbols
                    for symbol in removed_symbols:
                        pipe.hdel(self.WATCHLIST_DETAILS_KEY, symbol)

                pipe.execute()

            # Update details for all stocks
            if stocks:
                details = {stock.symbol: json.dumps(stock.to_dict()) for stock in stocks}
                self._client.hset(self.WATCHLIST_DETAILS_KEY, mapping=details)

            if added_symbols:
                logger.info(f"Added {len(added_symbols)} symbols to watchlist: {sorted(added_symbols)}")
            if removed_symbols:
                logger.info(f"Removed {len(removed_symbols)} symbols from watchlist: {sorted(removed_symbols)}")

            return sorted(list(added_symbols)), sorted(list(removed_symbols))

        except redis.RedisError as e:
            logger.error(f"Failed to sync watchlist: {e}")
            raise

    def get_symbols(self) -> Set[str]:
        """
        Get all symbols in the watchlist.

        Returns:
            Set of stock symbols.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            return self._client.smembers(self.WATCHLIST_KEY)
        except redis.RedisError as e:
            logger.error(f"Failed to get watchlist symbols: {e}")
            return set()

    def get_symbol_details(self, symbol: str) -> Optional[dict]:
        """
        Get details for a specific symbol.

        Args:
            symbol: The stock symbol.

        Returns:
            Symbol details dict or None if not found.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            data = self._client.hget(self.WATCHLIST_DETAILS_KEY, symbol)
            if data:
                return json.loads(data)
            return None
        except redis.RedisError as e:
            logger.error(f"Failed to get symbol details: {e}")
            return None

    def get_all_details(self) -> dict:
        """
        Get details for all symbols in the watchlist.

        Returns:
            Dictionary mapping symbol to details dict.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            data = self._client.hgetall(self.WATCHLIST_DETAILS_KEY)
            return {symbol: json.loads(details_json) for symbol, details_json in data.items()}
        except redis.RedisError as e:
            logger.error(f"Failed to get all symbol details: {e}")
            return {}

    def add_symbol(self, symbol: str, name: str = "") -> bool:
        """
        Manually add a symbol to the watchlist.

        Args:
            symbol: Stock symbol to add.
            name: Optional company name.

        Returns:
            True if symbol was newly added, False if it already existed.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            # Check if already exists
            already_exists = self._client.sismember(self.WATCHLIST_KEY, symbol)

            # Add to set
            self._client.sadd(self.WATCHLIST_KEY, symbol)

            # Store basic details
            from datetime import datetime, timezone
            details = {
                "symbol": symbol,
                "name": name or symbol,
                "instrument_url": "",
                "added_at": datetime.now(timezone.utc).isoformat(),
            }
            self._client.hset(self.WATCHLIST_DETAILS_KEY, symbol, json.dumps(details))

            if not already_exists:
                logger.info(f"Added symbol {symbol} to watchlist")

            return not already_exists

        except redis.RedisError as e:
            logger.error(f"Failed to add symbol: {e}")
            return False

    def remove_symbol(self, symbol: str) -> bool:
        """
        Remove a symbol from the watchlist.

        Args:
            symbol: Stock symbol to remove.

        Returns:
            True if symbol was removed, False if it didn't exist.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            removed = self._client.srem(self.WATCHLIST_KEY, symbol)
            self._client.hdel(self.WATCHLIST_DETAILS_KEY, symbol)

            if removed:
                logger.info(f"Removed symbol {symbol} from watchlist")

            return bool(removed)

        except redis.RedisError as e:
            logger.error(f"Failed to remove symbol: {e}")
            return False

    def symbol_exists(self, symbol: str) -> bool:
        """
        Check if a symbol is in the watchlist.

        Args:
            symbol: Stock symbol to check.

        Returns:
            True if symbol exists in watchlist.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            return self._client.sismember(self.WATCHLIST_KEY, symbol)
        except redis.RedisError as e:
            logger.error(f"Failed to check symbol: {e}")
            return False

    def count(self) -> int:
        """
        Get the number of symbols in the watchlist.

        Returns:
            Number of symbols.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            return self._client.scard(self.WATCHLIST_KEY)
        except redis.RedisError as e:
            logger.error(f"Failed to count symbols: {e}")
            return 0
