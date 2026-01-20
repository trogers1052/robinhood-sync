"""
Robinhood API client wrapper using robin_stocks library.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from dataclasses import dataclass
from decimal import Decimal

import robin_stocks.robinhood as rh
from dateutil import parser as date_parser

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Represents a completed trade from Robinhood."""

    order_id: str
    symbol: str
    side: str  # "buy" or "sell"
    quantity: Decimal
    average_price: Decimal
    total_notional: Decimal
    fees: Decimal
    state: str  # "filled", "cancelled", etc.
    executed_at: Optional[datetime]
    created_at: datetime
    instrument_url: str

    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": str(self.quantity),
            "average_price": str(self.average_price),
            "total_notional": str(self.total_notional),
            "fees": str(self.fees),
            "state": self.state,
            "executed_at": self.executed_at.isoformat() if self.executed_at else None,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class Position:
    """Represents a current position (holding) from Robinhood."""

    symbol: str
    quantity: Decimal
    average_buy_price: Decimal
    equity: Decimal  # current market value
    percent_change: Decimal
    equity_change: Decimal  # unrealized P&L in dollars
    updated_at: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka/Redis serialization."""
        return {
            "symbol": self.symbol,
            "quantity": str(self.quantity),
            "average_buy_price": str(self.average_buy_price),
            "equity": str(self.equity),
            "percent_change": str(self.percent_change),
            "equity_change": str(self.equity_change),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class AccountBalance:
    """Represents account balance and buying power from Robinhood."""

    buying_power: Decimal
    cash: Decimal
    total_equity: Decimal
    updated_at: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka/Redis serialization."""
        return {
            "buying_power": str(self.buying_power),
            "cash": str(self.cash),
            "total_equity": str(self.total_equity),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class WatchlistStock:
    """Represents a stock in the Robinhood watchlist."""

    symbol: str
    name: str
    instrument_url: str
    added_at: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka/Redis serialization."""
        return {
            "symbol": self.symbol,
            "name": self.name,
            "instrument_url": self.instrument_url,
            "added_at": self.added_at.isoformat(),
        }


class RobinhoodClient:
    """Client for interacting with Robinhood API."""

    def __init__(
        self,
        username: str,
        password: str,
        totp_secret: Optional[str] = None,
    ):
        self.username = username
        self.password = password
        self.totp_secret = totp_secret
        self._logged_in = False
        self._instrument_cache: dict[str, str] = {}  # URL -> symbol

    def login(self) -> bool:
        """
        Authenticate with Robinhood.

        Returns:
            True if login successful, False otherwise.
        """
        try:
            logger.info(f"Logging in to Robinhood as {self.username}...")

            # Generate TOTP code if secret provided
            mfa_code = None
            if self.totp_secret:
                import pyotp
                totp = pyotp.TOTP(self.totp_secret)
                mfa_code = totp.now()
                logger.info("Generated TOTP code for 2FA")

            # Login with robin_stocks
            login_result = rh.login(
                username=self.username,
                password=self.password,
                mfa_code=mfa_code,
                store_session=True,  # Store session to avoid re-login
            )

            if login_result:
                self._logged_in = True
                logger.info("Successfully logged in to Robinhood")
                return True
            else:
                logger.error("Failed to login to Robinhood")
                return False

        except Exception as e:
            logger.error(f"Error during Robinhood login: {e}")
            return False

    def logout(self) -> None:
        """Logout from Robinhood."""
        try:
            rh.logout()
            self._logged_in = False
            logger.info("Logged out from Robinhood")
        except Exception as e:
            logger.error(f"Error during logout: {e}")

    def is_logged_in(self) -> bool:
        """Check if currently logged in."""
        return self._logged_in

    def _get_symbol_from_instrument(self, instrument_url: str) -> str:
        """
        Get stock symbol from instrument URL.

        Robinhood orders don't include the symbol directly, so we need to
        fetch it from the instrument URL. Results are cached.
        """
        if instrument_url in self._instrument_cache:
            return self._instrument_cache[instrument_url]

        try:
            instrument_data = rh.stocks.get_instrument_by_url(instrument_url)
            symbol = instrument_data.get("symbol", "UNKNOWN")
            self._instrument_cache[instrument_url] = symbol
            return symbol
        except Exception as e:
            logger.error(f"Failed to get symbol for instrument {instrument_url}: {e}")
            return "UNKNOWN"

    def get_all_orders(self) -> list[Trade]:
        """
        Get all stock orders from Robinhood.

        Returns:
            List of Trade objects for all orders.
        """
        if not self._logged_in:
            raise RuntimeError("Not logged in to Robinhood")

        try:
            logger.info("Fetching all stock orders from Robinhood...")
            orders = rh.orders.get_all_stock_orders()

            if not orders:
                logger.info("No orders found")
                return []

            trades = []
            total_orders = len(orders)
            logger.info(f"Processing {total_orders} orders...")

            for i, order in enumerate(orders):
                try:
                    trade = self._parse_order(order)
                    if trade:
                        trades.append(trade)

                    # Log progress every 50 orders
                    if (i + 1) % 50 == 0:
                        logger.info(f"Processed {i + 1}/{total_orders} orders...")

                except Exception as e:
                    logger.warning(f"Failed to parse order {order.get('id', 'unknown')}: {e}")
                    continue

            logger.info(f"Successfully parsed {len(trades)} trades from {total_orders} orders")
            return trades

        except Exception as e:
            logger.error(f"Error fetching orders: {e}")
            raise

    def get_filled_orders(self, since_days: Optional[int] = None) -> list[Trade]:
        """
        Get only filled (completed) orders.

        Args:
            since_days: Only return orders from the last N days. None for all.

        Returns:
            List of filled Trade objects.
        """
        all_trades = self.get_all_orders()

        # Filter to filled orders only
        filled_trades = [t for t in all_trades if t.state == "filled"]

        # Optionally filter by date
        if since_days is not None:
            # Use timezone-aware datetime for comparison
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=since_days)

            def is_recent(trade: Trade) -> bool:
                if not trade.executed_at:
                    return False
                exec_time = trade.executed_at
                # Make naive datetime timezone-aware (assume UTC)
                if exec_time.tzinfo is None:
                    exec_time = exec_time.replace(tzinfo=timezone.utc)
                return exec_time >= cutoff_date

            filled_trades = [t for t in filled_trades if is_recent(t)]

        # Sort by executed_at ascending (oldest first) so BUYs are processed before SELLs
        # This ensures positions are created before they can be closed
        filled_trades.sort(key=lambda t: t.executed_at or t.created_at)

        logger.info(f"Found {len(filled_trades)} filled trades (sorted oldest to newest)")
        return filled_trades

    def _parse_order(self, order: dict) -> Optional[Trade]:
        """
        Parse a Robinhood order dict into a Trade object.

        Args:
            order: Raw order dictionary from Robinhood API.

        Returns:
            Trade object or None if parsing fails.
        """
        try:
            # Handle None or non-dict orders
            if not order or not isinstance(order, dict):
                logger.debug("Skipping invalid order (None or not a dict)")
                return None

            order_id = order.get("id")
            if not order_id:
                logger.debug("Skipping order without ID")
                return None

            # Get the symbol from instrument URL
            instrument_url = order.get("instrument") or ""
            if not instrument_url:
                logger.debug(f"Order {order_id} has no instrument URL")
                return None

            symbol = self._get_symbol_from_instrument(instrument_url)

            # Parse side (buy/sell)
            side = (order.get("side") or "").lower()

            # Parse quantities and prices - handle None values
            cumulative_qty = order.get("cumulative_quantity")
            quantity = Decimal(cumulative_qty if cumulative_qty else "0")

            avg_price = order.get("average_price")
            average_price = Decimal(avg_price if avg_price else "0")

            # Calculate total notional (may also be provided directly)
            # Handle case where executed_notional is None
            executed_notional = order.get("executed_notional")
            if executed_notional and isinstance(executed_notional, dict):
                notional_amount = executed_notional.get("amount")
                total_notional = Decimal(notional_amount if notional_amount else "0")
            else:
                total_notional = Decimal("0")

            if total_notional == 0:
                total_notional = quantity * average_price

            # Fees (usually 0 for Robinhood) - handle None
            fees_val = order.get("fees")
            fees = Decimal(fees_val if fees_val else "0")

            # State
            state = order.get("state") or "unknown"

            # Timestamps
            executed_at = None
            executions = order.get("executions")
            if executions and isinstance(executions, list) and len(executions) > 0:
                # Get the timestamp of the last execution
                last_execution = executions[-1]
                if last_execution and isinstance(last_execution, dict):
                    timestamp = last_execution.get("timestamp")
                    if timestamp:
                        executed_at = date_parser.parse(timestamp)

            # Fallback to last_transaction_at if no execution timestamp
            if not executed_at:
                last_transaction = order.get("last_transaction_at")
                if last_transaction:
                    executed_at = date_parser.parse(last_transaction)

            # Created at timestamp
            created_at_str = order.get("created_at")
            if created_at_str:
                created_at = date_parser.parse(created_at_str)
            else:
                created_at = datetime.now()

            return Trade(
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                average_price=average_price,
                total_notional=total_notional,
                fees=fees,
                state=state,
                executed_at=executed_at,
                created_at=created_at,
                instrument_url=instrument_url,
            )

        except Exception as e:
            order_id = order.get("id", "unknown") if isinstance(order, dict) else "unknown"
            logger.warning(f"Error parsing order {order_id}: {e}")
            return None

    def get_current_positions(self) -> list[Position]:
        """
        Get current stock positions (holdings) from Robinhood.

        Returns:
            List of Position objects representing current holdings.
        """
        if not self._logged_in:
            raise RuntimeError("Not logged in to Robinhood")

        try:
            logger.info("Fetching current positions from Robinhood...")

            # build_holdings() returns a dict keyed by symbol with position details
            holdings = rh.account.build_holdings()

            if not holdings:
                logger.info("No positions found")
                return []

            positions = []
            now = datetime.now(timezone.utc)

            for symbol, data in holdings.items():
                try:
                    position = Position(
                        symbol=symbol,
                        quantity=Decimal(str(data.get("quantity", "0"))),
                        average_buy_price=Decimal(str(data.get("average_buy_price", "0"))),
                        equity=Decimal(str(data.get("equity", "0"))),
                        percent_change=Decimal(str(data.get("percent_change", "0"))),
                        equity_change=Decimal(str(data.get("equity_change", "0"))),
                        updated_at=now,
                    )
                    positions.append(position)
                except Exception as e:
                    logger.warning(f"Failed to parse position for {symbol}: {e}")
                    continue

            logger.info(f"Found {len(positions)} current positions")
            return positions

        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            raise

    def get_account_balance(self) -> AccountBalance:
        """
        Get account balance and buying power from Robinhood.

        Returns:
            AccountBalance object with buying power, cash, and total equity.
        """
        if not self._logged_in:
            raise RuntimeError("Not logged in to Robinhood")

        try:
            logger.info("Fetching account balance from Robinhood...")

            # Get account profile for balance info
            profile = rh.profiles.load_account_profile()

            if not profile:
                raise RuntimeError("Failed to load account profile")

            # Get portfolio info for total equity
            portfolio = rh.profiles.load_portfolio_profile()

            buying_power = Decimal(str(profile.get("buying_power", "0")))
            cash = Decimal(str(profile.get("cash", "0")))

            # Total equity comes from portfolio
            total_equity = Decimal("0")
            if portfolio:
                total_equity = Decimal(str(portfolio.get("equity", "0")))

            balance = AccountBalance(
                buying_power=buying_power,
                cash=cash,
                total_equity=total_equity,
                updated_at=datetime.now(timezone.utc),
            )

            logger.info(
                f"Account balance: buying_power=${buying_power}, "
                f"cash=${cash}, total_equity=${total_equity}"
            )
            return balance

        except Exception as e:
            logger.error(f"Error fetching account balance: {e}")
            raise

    def get_watchlist(self, watchlist_name: str = "Default") -> list[WatchlistStock]:
        """
        Get stocks from a Robinhood watchlist.

        Args:
            watchlist_name: Name of the watchlist (default: "Default")

        Returns:
            List of WatchlistStock objects.
        """
        if not self._logged_in:
            raise RuntimeError("Not logged in to Robinhood")

        try:
            logger.info(f"Fetching watchlist '{watchlist_name}' from Robinhood...")

            # Get all watchlists
            watchlists = rh.account.get_all_watchlists()

            if not watchlists:
                logger.info("No watchlists found")
                return []

            # Find the requested watchlist - handle both dict and string formats
            target_watchlist = None
            available_names = []
            for wl in watchlists:
                if isinstance(wl, dict):
                    wl_name = wl.get("display_name")
                    available_names.append(wl_name)
                    if wl_name == watchlist_name:
                        target_watchlist = wl
                        break
                elif isinstance(wl, str):
                    # If watchlists are returned as strings, they might be names
                    available_names.append(wl)
                    if wl == watchlist_name:
                        target_watchlist = {"display_name": wl}
                        break

            if not target_watchlist:
                logger.warning(f"Watchlist '{watchlist_name}' not found")
                logger.info(f"Available watchlists: {available_names}")
                return []

            # Fetch watchlist items using robin_stocks
            # get_watchlist_by_name returns instrument data
            items = rh.account.get_watchlist_by_name(name=watchlist_name)

            logger.debug(f"get_watchlist_by_name returned type: {type(items)}, length: {len(items) if items else 0}")
            if items:
                logger.debug(f"First item type: {type(items[0]) if items else 'N/A'}")

            if not items:
                logger.info(f"Watchlist '{watchlist_name}' is empty")
                return []

            stocks = []
            now = datetime.now(timezone.utc)

            for item in items:
                try:
                    # robin_stocks may return either:
                    # - a list of strings (instrument URLs directly)
                    # - a list of dicts with an "instrument" key
                    # - a list of symbols as strings
                    instrument_url = None
                    symbol = None

                    if isinstance(item, str):
                        # Could be an instrument URL or a symbol
                        if item.startswith("http"):
                            instrument_url = item
                        else:
                            # Assume it's a symbol
                            symbol = item
                    elif isinstance(item, dict):
                        instrument_url = item.get("instrument")
                        # Some responses might have symbol directly
                        if not instrument_url:
                            symbol = item.get("symbol")
                    else:
                        logger.debug(f"Skipping unexpected item type: {type(item)}")
                        continue

                    # If we have an instrument URL, fetch the details
                    if instrument_url:
                        instrument = rh.stocks.get_instrument_by_url(instrument_url)
                        if instrument and isinstance(instrument, dict):
                            symbol = instrument.get("symbol", "UNKNOWN")
                            name = instrument.get("simple_name") or instrument.get("name", symbol)
                        else:
                            continue
                    elif symbol:
                        # We have a symbol directly, use it
                        name = symbol
                        instrument_url = ""
                    else:
                        continue

                    if not symbol:
                        continue

                    # Parse created_at if available (only when item is a dict)
                    added_at = now
                    if isinstance(item, dict):
                        created_at_str = item.get("created_at")
                        if created_at_str:
                            added_at = date_parser.parse(created_at_str)

                    stock = WatchlistStock(
                        symbol=symbol,
                        name=name,
                        instrument_url=instrument_url or "",
                        added_at=added_at,
                    )
                    stocks.append(stock)

                except Exception as e:
                    logger.warning(f"Failed to parse watchlist item {item}: {e}")
                    continue

            logger.info(f"Found {len(stocks)} stocks in watchlist '{watchlist_name}'")
            return stocks

        except Exception as e:
            logger.error(f"Error fetching watchlist: {e}")
            raise

    def get_all_watchlist_symbols(self) -> list[str]:
        """
        Get all unique symbols from all watchlists.

        Returns:
            List of unique stock symbols across all watchlists.
        """
        if not self._logged_in:
            raise RuntimeError("Not logged in to Robinhood")

        try:
            logger.info("Fetching all watchlist symbols...")

            # Get all watchlists
            watchlists = rh.account.get_all_watchlists()

            if not watchlists:
                logger.info("No watchlists found")
                return []

            all_symbols = set()

            for wl in watchlists:
                watchlist_name = wl.get("display_name", "Unknown")
                try:
                    stocks = self.get_watchlist(watchlist_name)
                    for stock in stocks:
                        all_symbols.add(stock.symbol)
                except Exception as e:
                    logger.warning(f"Failed to fetch watchlist '{watchlist_name}': {e}")
                    continue

            symbols_list = sorted(list(all_symbols))
            logger.info(f"Found {len(symbols_list)} unique symbols across all watchlists")
            return symbols_list

        except Exception as e:
            logger.error(f"Error fetching watchlist symbols: {e}")
            raise
