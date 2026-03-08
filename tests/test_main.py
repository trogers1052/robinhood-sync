"""Tests for main module — run_once, signal handling, health server."""

import logging
from unittest.mock import Mock, patch, MagicMock

import pytest

import robinhood_sync.main as main
from robinhood_sync.config import Settings


@pytest.fixture(autouse=True)
def reset_shutdown():
    main._shutdown_requested = False
    yield
    main._shutdown_requested = False


@pytest.fixture
def mock_settings():
    s = Mock(spec=Settings)
    s.market_open_hour = 4
    s.market_close_hour = 20
    s.poll_interval_minutes = 10
    s.sync_history_days = 30
    return s


def _mock_service():
    svc = Mock()
    svc.initialize.return_value = True
    svc.cleanup.return_value = None
    svc.sync_trades.return_value = (5, 2)
    svc.sync_positions.return_value = True
    svc.sync_watchlist.return_value = (1, 0)
    svc.sync_stop_orders.return_value = 3
    svc.sync_earnings_calendar.return_value = 10
    svc.is_healthy.return_value = True
    svc.reconnect.return_value = True
    svc.get_sync_stats.return_value = {"total_synced_orders": 42}
    return svc


# ---------------------------------------------------------------------------
# run_once
# ---------------------------------------------------------------------------


class TestRunOnce:
    def test_success(self, mock_settings):
        svc = _mock_service()
        with patch.object(main, "TradeSyncService", return_value=svc):
            rc = main.run_once(mock_settings)
        assert rc == 0
        svc.sync_trades.assert_called_once()
        svc.sync_positions.assert_called_once()
        svc.sync_watchlist.assert_called_once()
        svc.sync_stop_orders.assert_called_once()
        svc.sync_earnings_calendar.assert_called_once()
        svc.cleanup.assert_called_once()

    def test_init_failure(self, mock_settings):
        svc = _mock_service()
        svc.initialize.return_value = False
        with patch.object(main, "TradeSyncService", return_value=svc):
            rc = main.run_once(mock_settings)
        assert rc == 1

    def test_since_days_passed(self, mock_settings):
        svc = _mock_service()
        with patch.object(main, "TradeSyncService", return_value=svc):
            main.run_once(mock_settings, since_days=7)
        svc.sync_trades.assert_called_once_with(since_days=7)

    def test_exception_returns_1(self, mock_settings):
        svc = _mock_service()
        svc.sync_trades.side_effect = Exception("boom")
        with patch.object(main, "TradeSyncService", return_value=svc):
            rc = main.run_once(mock_settings)
        assert rc == 1
        svc.cleanup.assert_called_once()  # cleanup always runs

    def test_cleanup_always_called(self, mock_settings):
        svc = _mock_service()
        svc.initialize.return_value = False
        with patch.object(main, "TradeSyncService", return_value=svc):
            main.run_once(mock_settings)
        svc.cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# run_continuous — initial sync
# ---------------------------------------------------------------------------


class TestRunContinuousInitial:
    def test_initial_sync_runs_all(self, mock_settings):
        main._shutdown_requested = True  # Skip main loop
        svc = _mock_service()
        scheduler = Mock()
        with patch.object(main, "TradeSyncService", return_value=svc), \
             patch.object(main, "MarketScheduler", return_value=scheduler):
            rc = main.run_continuous(mock_settings)
        assert rc == 0
        svc.sync_trades.assert_called_once()
        svc.sync_positions.assert_called_once()
        svc.sync_watchlist.assert_called_once()
        svc.sync_stop_orders.assert_called_once()
        svc.sync_earnings_calendar.assert_called_once()

    def test_init_failure_returns_1(self, mock_settings):
        svc = _mock_service()
        svc.initialize.return_value = False
        scheduler = Mock()
        with patch.object(main, "TradeSyncService", return_value=svc), \
             patch.object(main, "MarketScheduler", return_value=scheduler):
            rc = main.run_continuous(mock_settings)
        assert rc == 1

    def test_initial_sync_exception_continues(self, mock_settings):
        """Initial sync exception is caught, loop continues (returns 0 since shutdown is True)."""
        main._shutdown_requested = True
        svc = _mock_service()
        svc.sync_trades.side_effect = Exception("initial boom")
        scheduler = Mock()
        with patch.object(main, "TradeSyncService", return_value=svc), \
             patch.object(main, "MarketScheduler", return_value=scheduler):
            rc = main.run_continuous(mock_settings)
        assert rc == 0  # Did not crash


# ---------------------------------------------------------------------------
# signal_handler
# ---------------------------------------------------------------------------


class TestSignalHandler:
    def test_sets_shutdown_flag(self):
        assert main._shutdown_requested is False
        main.signal_handler(2, None)
        assert main._shutdown_requested is True
