"""
Main sync logic for Robinhood trade synchronization.
"""

import logging
from datetime import date, datetime, timezone
from typing import Optional

from .config import Settings
from .robinhood_client import RobinhoodClient, Trade
from .kafka_producer import TradeEventProducer
from .redis_client import SyncedOrdersTracker, PositionStore, WatchlistStore, StopOrderStore, EarningsCalendarStore

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
        self.earnings_store: Optional[EarningsCalendarStore] = None

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

        # Initialize Redis earnings calendar store
        self.earnings_store = EarningsCalendarStore(self.settings)

        if not self.earnings_store.connect():
            logger.error("Failed to connect to Redis for earnings calendar store")
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

            # Publish to Kafka first.  Kafka is the durable event log; Redis
            # is a cache derived from it.  By writing Kafka first we ensure
            # that, on partial failure, downstream consumers (decision-engine,
            # etc.) see a consistent state: either both systems have the new
            # data or neither does.  If Kafka fails we abort before touching
            # Redis.  If Redis fails after a successful Kafka publish we log a
            # warning — Redis will be refreshed on the next sync cycle.
            if not self.kafka.publish_positions(positions, balance):
                logger.error(
                    "Failed to publish positions to Kafka — aborting Redis write "
                    "to keep both systems consistent"
                )
                return False

            # Kafka succeeded — now update the Redis cache.
            try:
                self.position_store.store_positions(positions)
                self.position_store.store_buying_power(balance)

                # Snapshot opening equity for daily loss circuit breaker.
                # Only writes on the first sync of each trading day (idempotent).
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if self.position_store.store_daily_equity_open(
                    balance.total_equity, today_str
                ):
                    logger.info(
                        f"Daily equity opening snapshot: ${balance.total_equity}"
                    )
            except Exception as redis_err:
                logger.warning(
                    f"Kafka publish succeeded but Redis cache update failed: {redis_err}. "
                    f"Redis will be refreshed on the next sync cycle."
                )
                # Still return True — the authoritative event has been published.

            logger.info(
                f"Positions sync complete: {len(positions)} positions, "
                f"buying power ${balance.buying_power}"
            )
            return True

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
        Sync watchlists from Robinhood.

        Fetches all configured watchlists, merges symbols, enriches with
        sector/industry from Robinhood fundamentals, syncs to Redis, and
        publishes Kafka events for changes.

        Returns:
            Tuple of (added_count, removed_count).
        """
        if not self.robinhood or not self.kafka or not self.watchlist_store:
            raise RuntimeError("Service not initialized")

        try:
            logger.info("Starting watchlist sync...")

            # Collect stocks from all configured watchlists
            all_stocks: dict[str, dict] = {}  # symbol -> stock dict (dedup)
            watchlist_names = self.settings.watchlist_name_list

            # Build case-insensitive lookup of watchlist name -> id
            wl_lookup = self.robinhood.get_all_watchlist_ids()

            for wl_name in watchlist_names:
                try:
                    wl_id = wl_lookup.get(wl_name.lower(), '')
                    if not wl_id:
                        logger.warning(f"Watchlist '{wl_name}' not found on Robinhood")
                        continue
                    wl_stocks = self.robinhood.get_watchlist_items_by_id(wl_id)
                    logger.info(f"Watchlist '{wl_name}': {len(wl_stocks)} symbols")
                    for stock in wl_stocks:
                        sym = stock.get('symbol')
                        if sym and sym not in all_stocks:
                            all_stocks[sym] = stock
                except Exception as e:
                    logger.warning(f"Failed to fetch watchlist '{wl_name}': {e}")

            stocks = list(all_stocks.values())
            logger.info(f"Total unique symbols across {len(watchlist_names)} watchlist(s): {len(stocks)}")

            if not stocks:
                logger.info("No stocks found in Robinhood watchlists")

            # Enrich with sector/industry from Robinhood fundamentals
            symbols = [s.get('symbol') for s in stocks if s.get('symbol')]
            if symbols:
                try:
                    fundamentals = self.robinhood.get_fundamentals(symbols)
                    enriched = 0
                    for stock in stocks:
                        sym = stock.get('symbol')
                        if sym and sym in fundamentals:
                            fund = fundamentals[sym]
                            if fund.get('sector'):
                                stock['sector'] = fund['sector']
                                enriched += 1
                            if fund.get('industry'):
                                stock['industry'] = fund['industry']
                    logger.info(f"Enriched {enriched}/{len(stocks)} symbols with sector/industry")
                except Exception as e:
                    logger.warning(f"Failed to enrich with fundamentals (non-fatal): {e}")

            # Sync to Redis and get changes
            added_symbols, removed_symbols = self.watchlist_store.sync_watchlist(stocks)

            # Publish Kafka events for changes
            if added_symbols or removed_symbols:
                all_symbols = sorted(s.get('symbol') for s in stocks if s.get('symbol'))
                self.kafka.publish_watchlist_update(
                    added_symbols=added_symbols,
                    removed_symbols=removed_symbols,
                    all_symbols=all_symbols,
                    stocks=stocks,
                )

                for symbol in added_symbols:
                    for s in stocks:
                        if s.get('symbol') == symbol:
                            name = s.get('name', symbol)
                            sector = s.get('sector', '')
                            industry = s.get('industry', '')
                            self.kafka.publish_symbol_added(symbol, name, sector, industry)
                            logger.info(f"Published symbol added event: {symbol} (sector={sector})")
                            break

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

    def sync_earnings_calendar(self) -> int:
        """
        Sync upcoming earnings dates for all watchlist + position symbols.

        For each symbol, calls the Robinhood earnings endpoint and stores the
        next upcoming quarter (eps.actual is None and report.date >= today) to
        Redis as ``robinhood:earnings:{SYMBOL}`` with a 24-hour TTL.

        ETFs (e.g. SPY, SLV) return an empty list — their Redis keys are
        cleared so the checklist knows there are no upcoming earnings.

        Returns:
            Number of symbols processed.
        """
        if not self.earnings_store or not self.watchlist_store or not self.position_store:
            raise RuntimeError("Service not initialized")

        try:
            logger.info("Starting earnings calendar sync...")

            # Collect all symbols: watchlist union positions
            symbols: set[str] = set()
            symbols.update(self.watchlist_store.get_symbols())
            positions = self.position_store.get_positions()
            symbols.update(positions.keys())

            if not symbols:
                logger.info("No symbols to sync earnings for")
                return 0

            today = date.today()
            processed = 0

            for symbol in sorted(symbols):
                try:
                    raw = self.robinhood.get_earnings(symbol)
                    if not raw:
                        # ETF or no earnings data — clear any stale key
                        self.earnings_store.clear_earnings(symbol)
                        logger.debug(f"No earnings data for {symbol} (likely ETF)")
                        processed += 1
                        continue

                    # Find the first quarter where eps.actual is None and
                    # report.date >= today — that is the next upcoming earnings
                    upcoming = None
                    for quarter in raw:
                        report = quarter.get("report") or {}
                        eps = quarter.get("eps") or {}
                        report_date_str = report.get("date", "")
                        actual = eps.get("actual")

                        if actual is not None:
                            # Already reported — skip
                            continue

                        if not report_date_str:
                            continue

                        try:
                            report_date = date.fromisoformat(report_date_str)
                        except ValueError:
                            continue

                        if report_date >= today:
                            if upcoming is None or report_date < date.fromisoformat(
                                upcoming["date"]
                            ):
                                upcoming = {
                                    "date": report_date_str,
                                    "timing": report.get("timing", ""),
                                    "verified": report.get("verified", False),
                                    "days_away": (report_date - today).days,
                                }

                    if upcoming:
                        self.earnings_store.store_next_earnings(symbol, upcoming)
                        logger.info(
                            f"Earnings {symbol}: {upcoming['date']} "
                            f"({upcoming['days_away']} days, "
                            f"verified={upcoming['verified']})"
                        )
                    else:
                        # No upcoming earnings found — clear stale key
                        self.earnings_store.clear_earnings(symbol)
                        logger.debug(f"No upcoming earnings found for {symbol}")

                    processed += 1

                except Exception as exc:
                    logger.warning(f"Failed to fetch earnings for {symbol}: {exc}")
                    processed += 1

            logger.info(f"Earnings calendar sync complete: {processed} symbols processed")
            return processed

        except Exception as e:
            logger.error(f"Error syncing earnings calendar: {e}")
            return 0

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

        if self.earnings_store:
            self.earnings_store.close()

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
