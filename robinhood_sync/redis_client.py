"""
Redis client for tracking synced order IDs, storing positions, and managing watchlist.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Set, TYPE_CHECKING

import redis

from .config import Settings

if TYPE_CHECKING:
    from .robinhood_client import Position, AccountBalance, WatchlistStock, StopOrder

logger = logging.getLogger(__name__)


class _RedisBase:
    """Base class providing Redis connection and reconnection logic."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._client: Optional[redis.Redis] = None

    def _create_client(self) -> redis.Redis:
        """Create a new Redis client instance."""
        return redis.Redis(
            host=self.settings.redis_host,
            port=self.settings.redis_port,
            password=self.settings.redis_password,
            db=self.settings.redis_db,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )

    def _reconnect(self) -> bool:
        """
        Attempt to reconnect to Redis by creating a new client.

        Returns:
            True if reconnection successful, False otherwise.
        """
        logger.warning(f"{self.__class__.__name__} attempting Redis reconnection...")
        try:
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass
            self._client = self._create_client()
            self._client.ping()
            logger.info(f"{self.__class__.__name__} reconnected to Redis")
            return True
        except redis.RedisError as e:
            logger.error(f"{self.__class__.__name__} reconnection failed: {e}")
            self._client = None
            return False

    def _with_retry(self, func, *args, **kwargs):
        """
        Execute a Redis operation with one reconnection retry on connection failure.

        Args:
            func: Callable that performs the Redis operation.
            *args: Positional arguments passed to func.
            **kwargs: Keyword arguments passed to func.

        Returns:
            The return value of func.

        Raises:
            The original exception if retry also fails.
        """
        try:
            return func(*args, **kwargs)
        except (redis.ConnectionError, redis.TimeoutError, ConnectionError, TimeoutError) as e:
            logger.warning(f"{self.__class__.__name__} Redis operation failed: {e}, retrying after reconnect")
            if self._reconnect():
                return func(*args, **kwargs)
            raise

    def close(self) -> None:
        """Close the Redis connection."""
        if self._client:
            self._client.close()
            logger.info(f"{self.__class__.__name__} Redis connection closed")


class SyncedOrdersTracker(_RedisBase):
    """Tracks which order IDs have been synced using Redis."""

    def __init__(self, settings: Settings):
        """
        Initialize the Redis client.

        Args:
            settings: Application settings containing Redis configuration.
        """
        super().__init__(settings)
        self.key = settings.redis_synced_orders_key

    def connect(self) -> bool:
        """
        Connect to Redis.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to Redis at {self.settings.redis_host}:{self.settings.redis_port}")

            self._client = self._create_client()

            # Test connection
            self._client.ping()

            count = self._client.scard(self.key)
            logger.info(f"Connected to Redis. Tracking {count} synced orders.")
            return True

        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False

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

        return self._with_retry(self._client.sismember, self.key, order_id)

    def mark_synced(self, order_id: str) -> None:
        """
        Mark an order as synced.

        Args:
            order_id: The order ID to mark as synced.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        self._with_retry(self._client.sadd, self.key, order_id)

    def mark_many_synced(self, order_ids: list[str]) -> None:
        """
        Mark multiple orders as synced.

        Args:
            order_ids: List of order IDs to mark as synced.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        if order_ids:
            self._with_retry(self._client.sadd, self.key, *order_ids)

    def get_all_synced(self) -> Set[str]:
        """
        Get all synced order IDs.

        Returns:
            Set of synced order IDs.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        return self._with_retry(self._client.smembers, self.key)

    def count_synced(self) -> int:
        """
        Get the count of synced orders.

        Returns:
            Number of synced orders.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        return self._with_retry(self._client.scard, self.key)

    def remove_synced(self, order_id: str) -> None:
        """
        Remove an order from the synced set (for re-processing).

        Args:
            order_id: The order ID to remove.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        self._with_retry(self._client.srem, self.key, order_id)

    def clear_all(self) -> None:
        """Clear all synced order IDs (use with caution)."""
        if not self._client:
            raise RuntimeError("Redis client not connected")

        self._with_retry(self._client.delete, self.key)
        logger.warning("Cleared all synced order IDs from Redis")


class PositionStore(_RedisBase):
    """Stores current positions and account balance in Redis."""

    POSITIONS_KEY = "robinhood:positions"
    BUYING_POWER_KEY = "robinhood:buying_power"
    DAILY_EQUITY_OPEN_KEY = "trading:daily_equity_open"
    POSITIONS_TTL = 3600  # 1 hour TTL for stale position cleanup
    DAILY_EQUITY_TTL = 86400  # 24 hours

    def __init__(self, settings: Settings):
        """
        Initialize the Redis client for position storage.

        Args:
            settings: Application settings containing Redis configuration.
        """
        super().__init__(settings)

    def connect(self) -> bool:
        """
        Connect to Redis.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"PositionStore connecting to Redis at {self.settings.redis_host}:{self.settings.redis_port}")

            self._client = self._create_client()

            # Test connection
            self._client.ping()
            logger.info("PositionStore connected to Redis")
            return True

        except redis.RedisError as e:
            logger.error(f"PositionStore failed to connect to Redis: {e}")
            return False

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

        def _do_store():
            if not positions:
                self._client.delete(self.POSITIONS_KEY)
                logger.info("No positions to store in Redis")
                return True

            position_data = {p.symbol: json.dumps(p.to_dict()) for p in positions}
            pipe = self._client.pipeline(transaction=True)
            pipe.delete(self.POSITIONS_KEY)
            pipe.hset(self.POSITIONS_KEY, mapping=position_data)
            pipe.expire(self.POSITIONS_KEY, self.POSITIONS_TTL)
            pipe.execute()

            logger.info(f"Stored {len(positions)} positions in Redis")
            return True

        try:
            return self._with_retry(_do_store)
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

        def _do_store():
            self._client.set(
                self.BUYING_POWER_KEY,
                json.dumps(balance.to_dict()),
                ex=self.POSITIONS_TTL,
            )
            logger.info(f"Stored buying power in Redis: ${balance.buying_power}")
            return True

        try:
            return self._with_retry(_do_store)
        except redis.RedisError as e:
            logger.error(f"Failed to store buying power in Redis: {e}")
            return False

    def store_daily_equity_open(self, equity: "Decimal", date_str: str) -> bool:
        """
        Store opening equity for the trading day (daily loss circuit breaker).

        Only writes if no snapshot exists for today — the first sync of the day
        sets the baseline.  Subsequent calls for the same date are no-ops.

        Args:
            equity: Total account equity from Robinhood.
            date_str: Date string in YYYY-MM-DD format.

        Returns:
            True if a new snapshot was written, False if today's already existed.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        def _do_store():
            existing = self._client.get(self.DAILY_EQUITY_OPEN_KEY)
            if existing:
                try:
                    data = json.loads(existing)
                    if data.get("date") == date_str:
                        return False  # already have today's snapshot
                except (json.JSONDecodeError, TypeError):
                    pass  # stale/corrupt data — overwrite

            payload = json.dumps({
                "equity": str(equity),
                "date": date_str,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            self._client.set(
                self.DAILY_EQUITY_OPEN_KEY, payload, ex=self.DAILY_EQUITY_TTL
            )
            return True

        try:
            return self._with_retry(_do_store)
        except redis.RedisError as e:
            logger.error(f"Failed to store daily equity open: {e}")
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
            data = self._with_retry(self._client.hgetall, self.POSITIONS_KEY)
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
            data = self._with_retry(self._client.get, self.BUYING_POWER_KEY)
            if data:
                return json.loads(data)
            return None
        except redis.RedisError as e:
            logger.error(f"Failed to get buying power from Redis: {e}")
            return None


class WatchlistStore(_RedisBase):
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
        super().__init__(settings)

    def connect(self) -> bool:
        """
        Connect to Redis.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"WatchlistStore connecting to Redis at {self.settings.redis_host}:{self.settings.redis_port}")

            self._client = self._create_client()

            # Test connection
            self._client.ping()

            count = self._client.scard(self.WATCHLIST_KEY)
            logger.info(f"WatchlistStore connected to Redis. {count} symbols in watchlist.")
            return True

        except redis.RedisError as e:
            logger.error(f"WatchlistStore failed to connect to Redis: {e}")
            return False

    _MAX_WATCH_RETRIES = 5
    _WATCH_RETRY_DELAY = 0.1  # seconds; doubles each retry

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

        new_symbols = {s.get('symbol') for s in stocks if s.get('symbol')}
        reconnect_attempted = False
        watch_retries = 0

        while True:
            try:
                pipe = self._client.pipeline(transaction=True)
                pipe.watch(self.WATCHLIST_KEY)
                current_symbols = self._client.smembers(self.WATCHLIST_KEY)

                added_symbols = new_symbols - current_symbols
                removed_symbols = current_symbols - new_symbols

                pipe.multi()

                if added_symbols:
                    pipe.sadd(self.WATCHLIST_KEY, *added_symbols)

                if removed_symbols:
                    pipe.srem(self.WATCHLIST_KEY, *removed_symbols)
                    for symbol in removed_symbols:
                        pipe.hdel(self.WATCHLIST_DETAILS_KEY, symbol)

                pipe.execute()

                if stocks:
                    details = {stock.get('symbol'): json.dumps(stock) for stock in stocks}
                    self._client.hset(self.WATCHLIST_DETAILS_KEY, mapping=details)

                if added_symbols:
                    logger.info(f"Added {len(added_symbols)} symbols to watchlist: {sorted(added_symbols)}")
                if removed_symbols:
                    logger.info(f"Removed {len(removed_symbols)} symbols from watchlist: {sorted(removed_symbols)}")

                return sorted(list(added_symbols)), sorted(list(removed_symbols))

            except redis.WatchError:
                watch_retries += 1
                if watch_retries > self._MAX_WATCH_RETRIES:
                    raise RuntimeError(
                        f"sync_watchlist failed after {self._MAX_WATCH_RETRIES} "
                        f"WatchError retries — persistent contention on {self.WATCHLIST_KEY}"
                    )
                delay = self._WATCH_RETRY_DELAY * (2 ** (watch_retries - 1))
                logger.warning(
                    f"WatchError on watchlist sync (attempt {watch_retries}/"
                    f"{self._MAX_WATCH_RETRIES}), retrying in {delay:.2f}s"
                )
                time.sleep(delay)
                continue
            except (redis.ConnectionError, redis.TimeoutError, ConnectionError, TimeoutError) as e:
                if not reconnect_attempted:
                    logger.warning(f"sync_watchlist connection error: {e}, attempting reconnect")
                    reconnect_attempted = True
                    if self._reconnect():
                        continue
                logger.error(f"Failed to sync watchlist: {e}")
                raise
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
            return self._with_retry(self._client.smembers, self.WATCHLIST_KEY)
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
            data = self._with_retry(self._client.hget, self.WATCHLIST_DETAILS_KEY, symbol)
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
            data = self._with_retry(self._client.hgetall, self.WATCHLIST_DETAILS_KEY)
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

        def _do_add():
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

        try:
            return self._with_retry(_do_add)
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

        def _do_remove():
            removed = self._client.srem(self.WATCHLIST_KEY, symbol)
            self._client.hdel(self.WATCHLIST_DETAILS_KEY, symbol)

            if removed:
                logger.info(f"Removed symbol {symbol} from watchlist")

            return bool(removed)

        try:
            return self._with_retry(_do_remove)
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
            return self._with_retry(self._client.sismember, self.WATCHLIST_KEY, symbol)
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
            return self._with_retry(self._client.scard, self.WATCHLIST_KEY)
        except redis.RedisError as e:
            logger.error(f"Failed to count symbols: {e}")
            return 0


class EarningsCalendarStore(_RedisBase):
    """
    Stores upcoming earnings dates per symbol in Redis.

    Key pattern: robinhood:earnings:{SYMBOL}
    Value: JSON blob — {"date": "YYYY-MM-DD", "timing": "am|pm",
                        "verified": bool, "days_away": int}
    TTL: 24 hours (refreshed on each sync, auto-expires if symbol removed)

    ETFs return an empty list from Robinhood — we simply skip them.
    Absent key → no known upcoming earnings (safe for checklist to treat as clear).
    """

    KEY_PREFIX = "robinhood:earnings"
    TTL = 86_400  # 24 hours

    def __init__(self, settings: Settings):
        super().__init__(settings)

    def connect(self) -> bool:
        try:
            self._client = self._create_client()
            self._client.ping()
            logger.info("EarningsCalendarStore connected to Redis")
            return True
        except redis.RedisError as e:
            logger.error(f"EarningsCalendarStore failed to connect to Redis: {e}")
            return False

    def store_next_earnings(self, symbol: str, earnings_data: dict) -> bool:
        """
        Store the next upcoming earnings date for a symbol.

        Args:
            symbol: Ticker symbol.
            earnings_data: Dict with keys: date, timing, verified, days_away.

        Returns:
            True if successful.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        def _do_store():
            key = f"{self.KEY_PREFIX}:{symbol}"
            self._client.set(key, json.dumps(earnings_data), ex=self.TTL)
            logger.debug(
                f"Stored earnings for {symbol}: {earnings_data['date']} "
                f"({earnings_data['days_away']} days away)"
            )
            return True

        try:
            return self._with_retry(_do_store)
        except redis.RedisError as e:
            logger.error(f"Failed to store earnings for {symbol}: {e}")
            return False

    def clear_earnings(self, symbol: str) -> None:
        """Remove the earnings key for a symbol (e.g. ETF with no earnings)."""
        if not self._client:
            return
        try:
            self._with_retry(self._client.delete, f"{self.KEY_PREFIX}:{symbol}")
        except redis.RedisError:
            pass

    def get_next_earnings(self, symbol: str) -> Optional[dict]:
        """Return the stored earnings data for a symbol, or None if absent."""
        if not self._client:
            raise RuntimeError("Redis client not connected")
        try:
            raw = self._with_retry(self._client.get, f"{self.KEY_PREFIX}:{symbol}")
            return json.loads(raw) if raw else None
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"Failed to get earnings for {symbol}: {e}")
            return None


class StopOrderStore(_RedisBase):
    """
    Stores pending stop loss orders in Redis.

    This allows stop-loss-guardian to know which positions have stop losses set.
    """

    STOP_ORDERS_KEY = "robinhood:stop_orders"  # Hash of symbol -> stop order JSON
    STOP_ORDERS_TTL = 3600  # 1 hour TTL

    def __init__(self, settings: Settings):
        """
        Initialize the Redis client for stop order storage.

        Args:
            settings: Application settings containing Redis configuration.
        """
        super().__init__(settings)

    def connect(self) -> bool:
        """
        Connect to Redis.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            logger.info(f"StopOrderStore connecting to Redis at {self.settings.redis_host}:{self.settings.redis_port}")

            self._client = self._create_client()

            # Test connection
            self._client.ping()
            logger.info("StopOrderStore connected to Redis")
            return True

        except redis.RedisError as e:
            logger.error(f"StopOrderStore failed to connect to Redis: {e}")
            return False

    def store_stop_orders(self, stop_orders: list["StopOrder"]) -> bool:
        """
        Store pending stop orders in Redis.

        Replaces all existing stop orders with the new snapshot.

        Args:
            stop_orders: List of StopOrder objects to store.

        Returns:
            True if successful, False otherwise.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        def _do_store():
            if not stop_orders:
                self._client.delete(self.STOP_ORDERS_KEY)
                logger.info("No stop orders to store in Redis")
                return True

            # If multiple stop orders for same symbol, keep the one with highest stop price
            orders_by_symbol = {}
            for order in stop_orders:
                existing = orders_by_symbol.get(order.symbol)
                if existing is None or order.stop_price > existing.stop_price:
                    orders_by_symbol[order.symbol] = order

            order_data = {symbol: json.dumps(order.to_dict()) for symbol, order in orders_by_symbol.items()}
            pipe = self._client.pipeline(transaction=True)
            pipe.delete(self.STOP_ORDERS_KEY)
            pipe.hset(self.STOP_ORDERS_KEY, mapping=order_data)
            pipe.expire(self.STOP_ORDERS_KEY, self.STOP_ORDERS_TTL)
            pipe.execute()

            logger.info(f"Stored {len(orders_by_symbol)} stop orders in Redis")
            return True

        try:
            return self._with_retry(_do_store)
        except redis.RedisError as e:
            logger.error(f"Failed to store stop orders in Redis: {e}")
            return False

    def get_stop_order(self, symbol: str) -> Optional[dict]:
        """
        Get stop order for a specific symbol.

        Args:
            symbol: Stock symbol.

        Returns:
            Stop order data as dict, or None if not found.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            data = self._with_retry(self._client.hget, self.STOP_ORDERS_KEY, symbol)
            if data:
                return json.loads(data)
            return None
        except redis.RedisError as e:
            logger.error(f"Failed to get stop order for {symbol}: {e}")
            return None

    def get_all_stop_orders(self) -> dict:
        """
        Get all stored stop orders from Redis.

        Returns:
            Dictionary mapping symbol to stop order data.
        """
        if not self._client:
            raise RuntimeError("Redis client not connected")

        try:
            data = self._with_retry(self._client.hgetall, self.STOP_ORDERS_KEY)
            return {symbol: json.loads(order_json) for symbol, order_json in data.items()}
        except redis.RedisError as e:
            logger.error(f"Failed to get stop orders from Redis: {e}")
            return {}
