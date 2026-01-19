import logging
from unittest.mock import Mock, patch, call

import pytest

import robinhood_sync.main as main
from robinhood_sync.config import Settings


@pytest.fixture(autouse=True)
def reset_shutdown_flag():
    """Ensure global shutdown flag doesn't leak between tests."""
    main._shutdown_requested = False
    yield
    main._shutdown_requested = False


@pytest.fixture
def mock_settings():
    settings = Mock(spec=Settings)
    settings.market_open_hour = 4
    settings.market_close_hour = 20
    settings.poll_interval_minutes = 10
    settings.sync_history_days = 30
    return settings


def _mock_service():
    service = Mock()
    service.initialize.return_value = True
    service.cleanup.return_value = None

    # Methods referenced by run_continuous (even if loop doesn't run)
    service.is_healthy.return_value = True
    service.reconnect.return_value = True
    service.get_sync_stats.return_value = {}

    return service


def test_run_continuous_calls_sync_positions_after_initial_trade_sync(mock_settings):
    main._shutdown_requested = True  # Skip the main loop; exercise initial sync only

    service = _mock_service()
    service.sync_trades.return_value = (5, 1)
    service.sync_positions.return_value = None

    scheduler = Mock()

    with patch.object(main, "TradeSyncService", return_value=service), patch.object(
        main, "MarketScheduler", return_value=scheduler
    ):
        rc = main.run_continuous(mock_settings)

    assert rc == 0

    trades_call = call.sync_trades(since_days=mock_settings.sync_history_days)
    positions_call = call.sync_positions()

    assert trades_call in service.method_calls
    assert positions_call in service.method_calls
    assert service.method_calls.index(trades_call) < service.method_calls.index(positions_call)


def test_run_continuous_handles_sync_positions_exceptions_gracefully(mock_settings, caplog):
    main._shutdown_requested = True  # Skip the main loop

    service = _mock_service()
    service.sync_trades.return_value = (1, 0)
    service.sync_positions.side_effect = Exception("boom")

    scheduler = Mock()

    caplog.set_level(logging.ERROR)

    with patch.object(main, "TradeSyncService", return_value=service), patch.object(
        main, "MarketScheduler", return_value=scheduler
    ):
        rc = main.run_continuous(mock_settings)

    assert rc == 0
    assert "Initial sync failed" in caplog.text


def test_run_continuous_logs_info_after_syncing_current_positions(mock_settings, monkeypatch):
    main._shutdown_requested = True  # Skip the main loop

    service = _mock_service()
    service.sync_trades.return_value = (0, 0)

    positions_synced = {"done": False}

    def _sync_positions():
        positions_synced["done"] = True

    service.sync_positions.side_effect = _sync_positions

    info_messages: list[str] = []

    def _info(msg, *args, **kwargs):
        # This asserts the log happens *after* sync_positions() successfully returns.
        if msg == "Current positions synced":
            assert positions_synced["done"] is True
        info_messages.append(msg)

    monkeypatch.setattr(main.logger, "info", _info)

    scheduler = Mock()

    with patch.object(main, "TradeSyncService", return_value=service), patch.object(
        main, "MarketScheduler", return_value=scheduler
    ):
        rc = main.run_continuous(mock_settings)

    assert rc == 0
    assert "Current positions synced" in info_messages
