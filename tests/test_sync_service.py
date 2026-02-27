"""Tests for TradeSyncService — sync_trades, sync_positions, sync_stop_orders, health, cleanup."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch

import pytest

from robinhood_sync.robinhood_client import (
    Trade,
    Position,
    AccountBalance,
    StopOrder,
)
from robinhood_sync.sync import TradeSyncService
from robinhood_sync.config import Settings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_trade(order_id="t-001", symbol="AAPL", side="buy"):
    return Trade(
        order_id=order_id,
        symbol=symbol,
        side=side,
        quantity=Decimal("10"),
        average_price=Decimal("150.00"),
        total_notional=Decimal("1500.00"),
        fees=Decimal("0"),
        state="filled",
        executed_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
        created_at=datetime(2026, 2, 20, 14, 0, tzinfo=timezone.utc),
        instrument_url="https://api.robinhood.com/instruments/1/",
    )


def _make_position(symbol="AAPL"):
    return Position(
        symbol=symbol,
        quantity=Decimal("10"),
        average_buy_price=Decimal("150.00"),
        equity=Decimal("1600.00"),
        percent_change=Decimal("6.67"),
        equity_change=Decimal("100.00"),
        updated_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
    )


def _make_balance():
    return AccountBalance(
        buying_power=Decimal("500.00"),
        cash=Decimal("300.00"),
        total_equity=Decimal("1200.00"),
        updated_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
    )


def _make_stop_order(symbol="AAPL"):
    return StopOrder(
        order_id="s-001",
        symbol=symbol,
        stop_price=Decimal("140.00"),
        quantity=Decimal("10"),
        side="sell",
        order_type="market",
        limit_price=None,
        state="confirmed",
        created_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
    )


@pytest.fixture
def mock_settings():
    s = Mock(spec=Settings)
    s.robinhood_username = "user"
    s.robinhood_password = "pass"
    s.robinhood_totp_secret = None
    s.kafka_broker_list = ["localhost:9092"]
    s.kafka_topic = "trading.orders"
    s.kafka_positions_topic = "trading.positions"
    s.kafka_watchlist_topic = "trading.watchlist"
    s.redis_host = "localhost"
    s.redis_port = 6379
    s.redis_password = None
    s.redis_db = 0
    s.redis_synced_orders_key = "test:synced"
    s.watchlist_name = "Materials"
    s.watchlist_names = ""
    s.watchlist_name_list = ["Materials"]
    return s


def _wired_service(settings):
    """Create a TradeSyncService with all mocked dependencies."""
    svc = TradeSyncService(settings)
    svc.robinhood = Mock()
    svc.kafka = Mock()
    svc.tracker = Mock()
    svc.position_store = Mock()
    svc.watchlist_store = Mock()
    svc.stop_order_store = Mock()
    svc.earnings_store = Mock()
    return svc


# ---------------------------------------------------------------------------
# sync_trades
# ---------------------------------------------------------------------------


class TestSyncTrades:
    def test_not_initialized_raises(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        with pytest.raises(RuntimeError, match="not initialized"):
            svc.sync_trades()

    def test_no_filled_trades(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_filled_orders.return_value = []
        synced, skipped = svc.sync_trades()
        assert synced == 0
        assert skipped == 0

    def test_all_already_synced(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_filled_orders.return_value = [_make_trade()]
        svc.tracker.is_synced.return_value = True
        synced, skipped = svc.sync_trades()
        assert synced == 0
        assert skipped == 1

    def test_new_trades_synced(self, mock_settings):
        svc = _wired_service(mock_settings)
        trades = [_make_trade("t-1"), _make_trade("t-2")]
        svc.robinhood.get_filled_orders.return_value = trades
        svc.tracker.is_synced.return_value = False
        svc.kafka.publish_trade.return_value = True

        synced, skipped = svc.sync_trades()
        assert synced == 2
        assert skipped == 0
        assert svc.tracker.mark_synced.call_count == 2

    def test_mixed_synced_and_new(self, mock_settings):
        svc = _wired_service(mock_settings)
        trades = [_make_trade("t-1"), _make_trade("t-2"), _make_trade("t-3")]
        svc.robinhood.get_filled_orders.return_value = trades
        svc.tracker.is_synced.side_effect = lambda oid: oid == "t-2"
        svc.kafka.publish_trade.return_value = True

        synced, skipped = svc.sync_trades()
        assert synced == 2
        assert skipped == 1

    def test_kafka_publish_failure_not_marked(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_filled_orders.return_value = [_make_trade("t-1")]
        svc.tracker.is_synced.return_value = False
        svc.kafka.publish_trade.return_value = False

        synced, skipped = svc.sync_trades()
        assert synced == 0
        svc.tracker.mark_synced.assert_not_called()

    def test_passes_since_days(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_filled_orders.return_value = []
        svc.sync_trades(since_days=7)
        svc.robinhood.get_filled_orders.assert_called_once_with(since_days=7)


# ---------------------------------------------------------------------------
# sync_positions
# ---------------------------------------------------------------------------


class TestSyncPositions:
    def test_not_initialized_raises(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        with pytest.raises(RuntimeError, match="not initialized"):
            svc.sync_positions()

    def test_successful_sync(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_current_positions.return_value = [_make_position()]
        svc.robinhood.get_account_balance.return_value = _make_balance()
        svc.kafka.publish_positions.return_value = True

        result = svc.sync_positions()
        assert result is True
        svc.kafka.publish_positions.assert_called_once()
        svc.position_store.store_positions.assert_called_once()
        svc.position_store.store_buying_power.assert_called_once()

    def test_kafka_failure_aborts_redis_write(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_current_positions.return_value = [_make_position()]
        svc.robinhood.get_account_balance.return_value = _make_balance()
        svc.kafka.publish_positions.return_value = False

        result = svc.sync_positions()
        assert result is False
        # Redis should NOT be written to
        svc.position_store.store_positions.assert_not_called()

    def test_redis_failure_still_returns_true(self, mock_settings):
        """Kafka success + Redis failure = still True (Kafka is authoritative)."""
        svc = _wired_service(mock_settings)
        svc.robinhood.get_current_positions.return_value = [_make_position()]
        svc.robinhood.get_account_balance.return_value = _make_balance()
        svc.kafka.publish_positions.return_value = True
        svc.position_store.store_positions.side_effect = Exception("Redis down")

        result = svc.sync_positions()
        assert result is True


# ---------------------------------------------------------------------------
# sync_stop_orders
# ---------------------------------------------------------------------------


class TestSyncStopOrders:
    def test_not_initialized_raises(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        with pytest.raises(RuntimeError, match="not initialized"):
            svc.sync_stop_orders()

    def test_syncs_stop_orders(self, mock_settings):
        svc = _wired_service(mock_settings)
        stops = [_make_stop_order("AAPL"), _make_stop_order("GOOG")]
        svc.robinhood.get_stop_orders.return_value = stops

        count = svc.sync_stop_orders()
        assert count == 2
        svc.stop_order_store.store_stop_orders.assert_called_once_with(stops)

    def test_no_stop_orders(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_stop_orders.return_value = []
        count = svc.sync_stop_orders()
        assert count == 0

    def test_exception_returns_zero(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_stop_orders.side_effect = Exception("fail")
        count = svc.sync_stop_orders()
        assert count == 0


# ---------------------------------------------------------------------------
# is_healthy
# ---------------------------------------------------------------------------


class TestIsHealthy:
    def test_healthy(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.is_logged_in.return_value = True
        svc.tracker.count_synced.return_value = 42
        assert svc.is_healthy() is True

    def test_not_logged_in(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.is_logged_in.return_value = False
        assert svc.is_healthy() is False

    def test_no_robinhood(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        assert svc.is_healthy() is False

    def test_no_tracker(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        svc.robinhood = Mock()
        svc.robinhood.is_logged_in.return_value = True
        assert svc.is_healthy() is False

    def test_redis_error(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.is_logged_in.return_value = True
        svc.tracker.count_synced.side_effect = Exception("Redis down")
        assert svc.is_healthy() is False


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------


class TestCleanup:
    def test_cleanup_all(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.cleanup()
        svc.robinhood.logout.assert_called_once()
        svc.kafka.close.assert_called_once()
        svc.tracker.close.assert_called_once()
        svc.position_store.close.assert_called_once()
        svc.watchlist_store.close.assert_called_once()
        svc.stop_order_store.close.assert_called_once()
        svc.earnings_store.close.assert_called_once()

    def test_cleanup_none_components(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        svc.cleanup()  # Should not raise


# ---------------------------------------------------------------------------
# get_sync_stats
# ---------------------------------------------------------------------------


class TestGetSyncStats:
    def test_returns_stats(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.tracker.count_synced.return_value = 42
        stats = svc.get_sync_stats()
        assert stats["total_synced_orders"] == 42

    def test_not_initialized_raises(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        with pytest.raises(RuntimeError, match="not initialized"):
            svc.get_sync_stats()
