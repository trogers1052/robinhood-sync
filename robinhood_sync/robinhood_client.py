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
