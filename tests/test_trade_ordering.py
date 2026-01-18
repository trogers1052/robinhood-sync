"""
Tests for trade ordering - ensuring trades are processed oldest to newest.

This is critical for position tracking: BUYs must be processed before SELLs
to correctly open positions before closing them.
"""

import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional
from unittest.mock import Mock, MagicMock, patch, call

from robinhood_sync.robinhood_client import RobinhoodClient, Trade
from robinhood_sync.sync import TradeSyncService
from robinhood_sync.config import Settings


def create_trade(
    order_id: str,
    symbol: str,
    side: str,
    executed_at: datetime,
    created_at: Optional[datetime] = None,
) -> Trade:
    """Helper to create a Trade object for testing."""
    return Trade(
        order_id=order_id,
        symbol=symbol,
        side=side,
        quantity=Decimal("10"),
        average_price=Decimal("100.00"),
        total_notional=Decimal("1000.00"),
        fees=Decimal("0"),
        state="filled",
        executed_at=executed_at,
        created_at=created_at or executed_at,
        instrument_url="https://api.robinhood.com/instruments/test/",
    )


class TestTradeOrdering:
    """Tests for trade ordering in get_filled_orders."""

    def test_trades_sorted_oldest_to_newest(self):
        """Trades should be sorted by executed_at, oldest first."""
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        # Create trades in reverse chronological order (how Robinhood returns them)
        now = datetime.now(timezone.utc)
        trade_newest = create_trade("3", "AAPL", "sell", now)
        trade_middle = create_trade("2", "AAPL", "buy", now - timedelta(days=1))
        trade_oldest = create_trade("1", "AAPL", "buy", now - timedelta(days=2))

        # Mock get_all_orders to return trades newest first (like Robinhood API)
        with patch.object(client, "get_all_orders") as mock_get_all:
            mock_get_all.return_value = [trade_newest, trade_middle, trade_oldest]

            result = client.get_filled_orders()

            # Should be sorted oldest to newest
            assert len(result) == 3
            assert result[0].order_id == "1", "Oldest trade should be first"
            assert result[1].order_id == "2", "Middle trade should be second"
            assert result[2].order_id == "3", "Newest trade should be last"

    def test_trades_sorted_with_date_filter(self):
        """Trades should still be sorted when filtered by date."""
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        now = datetime.now(timezone.utc)
        # Only trades within 7 days should be included
        trade_recent_1 = create_trade("2", "AAPL", "sell", now - timedelta(days=1))
        trade_recent_2 = create_trade("1", "AAPL", "buy", now - timedelta(days=3))
        trade_old = create_trade("0", "AAPL", "buy", now - timedelta(days=30))

        with patch.object(client, "get_all_orders") as mock_get_all:
            # Return in reverse order
            mock_get_all.return_value = [trade_recent_1, trade_recent_2, trade_old]

            result = client.get_filled_orders(since_days=7)

            # Old trade should be filtered out, remaining sorted oldest first
            assert len(result) == 2
            assert result[0].order_id == "1", "Older recent trade should be first"
            assert result[1].order_id == "2", "Newer recent trade should be last"

    def test_trades_with_same_timestamp_stable_sort(self):
        """Trades with same timestamp should maintain stable ordering."""
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        same_time = datetime.now(timezone.utc)
        trade_a = create_trade("A", "AAPL", "buy", same_time)
        trade_b = create_trade("B", "AAPL", "buy", same_time)
        trade_c = create_trade("C", "AAPL", "buy", same_time)

        with patch.object(client, "get_all_orders") as mock_get_all:
            mock_get_all.return_value = [trade_a, trade_b, trade_c]

            result = client.get_filled_orders()

            # All should be present (order doesn't matter for same timestamp)
            assert len(result) == 3
            order_ids = [t.order_id for t in result]
            assert set(order_ids) == {"A", "B", "C"}

    def test_trades_with_none_executed_at_uses_created_at(self):
        """Trades without executed_at should fall back to created_at for sorting."""
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        now = datetime.now(timezone.utc)
        trade_with_exec = create_trade("1", "AAPL", "buy", now - timedelta(days=1))

        # Trade without executed_at but with created_at
        trade_no_exec = Trade(
            order_id="2",
            symbol="AAPL",
            side="buy",
            quantity=Decimal("10"),
            average_price=Decimal("100.00"),
            total_notional=Decimal("1000.00"),
            fees=Decimal("0"),
            state="filled",
            executed_at=None,
            created_at=now - timedelta(days=2),  # Older created_at
            instrument_url="https://api.robinhood.com/instruments/test/",
        )

        with patch.object(client, "get_all_orders") as mock_get_all:
            mock_get_all.return_value = [trade_with_exec, trade_no_exec]

            result = client.get_filled_orders()

            # Trade with older created_at (no executed_at) should be first
            assert len(result) == 2
            assert result[0].order_id == "2", "Trade with older created_at should be first"
            assert result[1].order_id == "1", "Trade with newer executed_at should be last"

    def test_buy_before_sell_for_same_symbol(self):
        """BUY trades should come before SELL trades when processed chronologically."""
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        now = datetime.now(timezone.utc)
        # Simulate a real trading scenario: buy, then sell
        buy_trade = create_trade("buy1", "AAPL", "buy", now - timedelta(hours=2))
        sell_trade = create_trade("sell1", "AAPL", "sell", now - timedelta(hours=1))

        with patch.object(client, "get_all_orders") as mock_get_all:
            # Robinhood returns newest first
            mock_get_all.return_value = [sell_trade, buy_trade]

            result = client.get_filled_orders()

            assert len(result) == 2
            assert result[0].side == "buy", "BUY should come first (executed earlier)"
            assert result[1].side == "sell", "SELL should come second (executed later)"


class TestSyncTradesOrdering:
    """Tests for the sync_trades method to verify Kafka publishing order."""

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings for testing."""
        settings = Mock(spec=Settings)
        settings.robinhood_username = "test_user"
        settings.robinhood_password = "test_pass"
        settings.robinhood_totp_secret = None
        settings.kafka_broker_list = ["localhost:9092"]
        settings.kafka_topic = "trading.orders"
        settings.redis_host = "localhost"
        settings.redis_port = 6379
        settings.redis_password = None
        settings.redis_db = 0
        settings.redis_synced_orders_key = "test:synced_orders"
        return settings

    def test_sync_publishes_trades_in_chronological_order(self, mock_settings):
        """sync_trades should publish trades oldest to newest."""
        service = TradeSyncService(mock_settings)

        now = datetime.now(timezone.utc)
        trade_old = create_trade("1", "AAPL", "buy", now - timedelta(days=2))
        trade_mid = create_trade("2", "AAPL", "buy", now - timedelta(days=1))
        trade_new = create_trade("3", "AAPL", "sell", now)

        # Mock dependencies
        service.robinhood = Mock()
        service.robinhood.get_filled_orders.return_value = [trade_old, trade_mid, trade_new]

        service.tracker = Mock()
        service.tracker.is_synced.return_value = False  # None are synced

        service.kafka = Mock()
        service.kafka.publish_trade.return_value = True

        # Run sync
        synced, skipped = service.sync_trades()

        # Verify publish order
        assert synced == 3
        assert skipped == 0

        # Check that publish_trade was called in correct order
        calls = service.kafka.publish_trade.call_args_list
        assert len(calls) == 3
        assert calls[0][0][0].order_id == "1", "First publish should be oldest trade"
        assert calls[1][0][0].order_id == "2", "Second publish should be middle trade"
        assert calls[2][0][0].order_id == "3", "Third publish should be newest trade"

    def test_sync_skips_already_synced_maintains_order(self, mock_settings):
        """Skipping synced trades should not affect order of remaining trades."""
        service = TradeSyncService(mock_settings)

        now = datetime.now(timezone.utc)
        trade_1 = create_trade("1", "AAPL", "buy", now - timedelta(days=3))
        trade_2 = create_trade("2", "AAPL", "buy", now - timedelta(days=2))
        trade_3 = create_trade("3", "AAPL", "sell", now - timedelta(days=1))

        service.robinhood = Mock()
        service.robinhood.get_filled_orders.return_value = [trade_1, trade_2, trade_3]

        service.tracker = Mock()
        # Trade 2 is already synced
        service.tracker.is_synced.side_effect = lambda oid: oid == "2"

        service.kafka = Mock()
        service.kafka.publish_trade.return_value = True

        synced, skipped = service.sync_trades()

        assert synced == 2
        assert skipped == 1

        # Only trades 1 and 3 should be published, in order
        calls = service.kafka.publish_trade.call_args_list
        assert len(calls) == 2
        assert calls[0][0][0].order_id == "1", "First should be trade 1"
        assert calls[1][0][0].order_id == "3", "Second should be trade 3"


class TestRealWorldScenarios:
    """Test real-world trading scenarios."""

    def test_multiple_buys_single_sell_scenario(self):
        """
        Scenario: User buys stock twice, then sells entire position.
        Order matters: both BUYs must be processed before SELL.
        """
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        now = datetime.now(timezone.utc)

        # User's actual trading history
        buy1 = create_trade("b1", "AAPL", "buy", now - timedelta(days=5))  # First buy
        buy2 = create_trade("b2", "AAPL", "buy", now - timedelta(days=3))  # Second buy
        sell = create_trade("s1", "AAPL", "sell", now - timedelta(days=1))  # Close position

        with patch.object(client, "get_all_orders") as mock_get_all:
            # Robinhood returns newest first
            mock_get_all.return_value = [sell, buy2, buy1]

            result = client.get_filled_orders()

            # Verify correct order for position tracking
            assert len(result) == 3
            assert result[0].order_id == "b1", "First buy should be first"
            assert result[1].order_id == "b2", "Second buy should be second"
            assert result[2].order_id == "s1", "Sell should be last"

            # Verify sides
            assert result[0].side == "buy"
            assert result[1].side == "buy"
            assert result[2].side == "sell"

    def test_multiple_symbols_interleaved(self):
        """
        Scenario: User trades multiple stocks with interleaved executions.
        Each symbol's trades should be in chronological order.
        """
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        now = datetime.now(timezone.utc)

        # Interleaved trades across symbols
        aapl_buy = create_trade("a1", "AAPL", "buy", now - timedelta(days=5))
        msft_buy = create_trade("m1", "MSFT", "buy", now - timedelta(days=4))
        aapl_sell = create_trade("a2", "AAPL", "sell", now - timedelta(days=3))
        msft_sell = create_trade("m2", "MSFT", "sell", now - timedelta(days=2))

        with patch.object(client, "get_all_orders") as mock_get_all:
            # Robinhood returns newest first
            mock_get_all.return_value = [msft_sell, aapl_sell, msft_buy, aapl_buy]

            result = client.get_filled_orders()

            # All should be sorted by execution time globally
            assert len(result) == 4
            expected_order = ["a1", "m1", "a2", "m2"]
            actual_order = [t.order_id for t in result]
            assert actual_order == expected_order, f"Expected {expected_order}, got {actual_order}"

    def test_partial_sells_scenario(self):
        """
        Scenario: User buys, partially sells, buys more, sells rest.
        """
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        now = datetime.now(timezone.utc)

        buy1 = create_trade("1", "AAPL", "buy", now - timedelta(days=10))
        partial_sell = create_trade("2", "AAPL", "sell", now - timedelta(days=7))
        buy2 = create_trade("3", "AAPL", "buy", now - timedelta(days=5))
        final_sell = create_trade("4", "AAPL", "sell", now - timedelta(days=1))

        with patch.object(client, "get_all_orders") as mock_get_all:
            mock_get_all.return_value = [final_sell, buy2, partial_sell, buy1]

            result = client.get_filled_orders()

            expected = ["1", "2", "3", "4"]
            actual = [t.order_id for t in result]
            assert actual == expected, f"Expected {expected}, got {actual}"


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_trades_list(self):
        """Empty trades list should return empty sorted list."""
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        with patch.object(client, "get_all_orders") as mock_get_all:
            mock_get_all.return_value = []

            result = client.get_filled_orders()

            assert result == []

    def test_single_trade(self):
        """Single trade should return that trade."""
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        now = datetime.now(timezone.utc)
        trade = create_trade("1", "AAPL", "buy", now)

        with patch.object(client, "get_all_orders") as mock_get_all:
            mock_get_all.return_value = [trade]

            result = client.get_filled_orders()

            assert len(result) == 1
            assert result[0].order_id == "1"

    def test_only_cancelled_orders_filtered_out(self):
        """Non-filled orders should be filtered before sorting."""
        client = RobinhoodClient("user", "pass")
        client._logged_in = True

        now = datetime.now(timezone.utc)

        filled = create_trade("1", "AAPL", "buy", now)
        cancelled = Trade(
            order_id="2",
            symbol="AAPL",
            side="buy",
            quantity=Decimal("10"),
            average_price=Decimal("100"),
            total_notional=Decimal("1000"),
            fees=Decimal("0"),
            state="cancelled",  # Not filled
            executed_at=now - timedelta(days=1),
            created_at=now - timedelta(days=1),
            instrument_url="https://api.robinhood.com/instruments/test/",
        )

        with patch.object(client, "get_all_orders") as mock_get_all:
            mock_get_all.return_value = [filled, cancelled]

            result = client.get_filled_orders()

            assert len(result) == 1
            assert result[0].order_id == "1"
            assert result[0].state == "filled"
