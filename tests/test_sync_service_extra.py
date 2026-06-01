"""Additional TradeSyncService tests: initialize, sync_watchlist,
sync_earnings_calendar, reconnect. All external clients mocked."""

from datetime import date, datetime, timezone, timedelta
from unittest.mock import Mock, MagicMock, patch

import pytest

from robinhood_sync.robinhood_client import LoginOutcome
from robinhood_sync.sync import TradeSyncService
from robinhood_sync.config import Settings


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
    s.watchlist_name_list = ["Materials"]
    return s


def _wired_service(settings):
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
# initialize
# ---------------------------------------------------------------------------

class TestInitialize:
    def _patch_all(self, login=LoginOutcome.SUCCESS, kafka=True,
                   tracker=True, pos=True, wl=True, stop=True, earn=True):
        rh = MagicMock()
        rh.login.return_value = login
        kp = MagicMock()
        kp.connect.return_value = kafka
        tracker_m = MagicMock(); tracker_m.connect.return_value = tracker
        pos_m = MagicMock(); pos_m.connect.return_value = pos
        wl_m = MagicMock(); wl_m.connect.return_value = wl
        stop_m = MagicMock(); stop_m.connect.return_value = stop
        earn_m = MagicMock(); earn_m.connect.return_value = earn
        patches = [
            patch("robinhood_sync.sync.RobinhoodClient", return_value=rh),
            patch("robinhood_sync.sync.TradeEventProducer", return_value=kp),
            patch("robinhood_sync.sync.SyncedOrdersTracker", return_value=tracker_m),
            patch("robinhood_sync.sync.PositionStore", return_value=pos_m),
            patch("robinhood_sync.sync.WatchlistStore", return_value=wl_m),
            patch("robinhood_sync.sync.StopOrderStore", return_value=stop_m),
            patch("robinhood_sync.sync.EarningsCalendarStore", return_value=earn_m),
        ]
        return patches

    def _run(self, settings, patches):
        for p in patches:
            p.start()
        try:
            return TradeSyncService(settings).initialize()
        finally:
            for p in patches:
                p.stop()

    def test_success(self, mock_settings):
        assert self._run(mock_settings, self._patch_all()) is True

    def test_login_failure(self, mock_settings):
        assert self._run(
            mock_settings, self._patch_all(login=LoginOutcome.BAD_CREDENTIALS)
        ) is False

    def test_kafka_failure(self, mock_settings):
        assert self._run(mock_settings, self._patch_all(kafka=False)) is False

    def test_tracker_failure(self, mock_settings):
        assert self._run(mock_settings, self._patch_all(tracker=False)) is False

    def test_position_store_failure(self, mock_settings):
        assert self._run(mock_settings, self._patch_all(pos=False)) is False

    def test_watchlist_store_failure(self, mock_settings):
        assert self._run(mock_settings, self._patch_all(wl=False)) is False

    def test_stop_order_store_failure(self, mock_settings):
        assert self._run(mock_settings, self._patch_all(stop=False)) is False

    def test_earnings_store_failure(self, mock_settings):
        assert self._run(mock_settings, self._patch_all(earn=False)) is False


# ---------------------------------------------------------------------------
# sync_watchlist
# ---------------------------------------------------------------------------

class TestSyncWatchlist:
    def test_not_initialized_raises(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        with pytest.raises(RuntimeError, match="not initialized"):
            svc.sync_watchlist()

    def test_full_sync_with_changes(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_all_watchlist_ids.return_value = {"materials": "wl1"}
        svc.robinhood.get_watchlist_items_by_id.return_value = [
            {"symbol": "AAPL", "name": "Apple"},
            {"symbol": "MSFT", "name": "Microsoft"},
        ]
        svc.robinhood.get_fundamentals.return_value = {
            "AAPL": {"sector": "Tech", "industry": "Hardware"},
        }
        svc.watchlist_store.sync_watchlist.return_value = (["AAPL"], ["OLD"])
        added, removed = svc.sync_watchlist()
        assert added == 1 and removed == 1
        svc.kafka.publish_watchlist_update.assert_called_once()
        svc.kafka.publish_symbol_added.assert_called_once()
        svc.kafka.publish_symbol_removed.assert_called_once()

    def test_watchlist_not_found(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_all_watchlist_ids.return_value = {}  # name not found
        svc.watchlist_store.sync_watchlist.return_value = ([], [])
        added, removed = svc.sync_watchlist()
        assert added == 0 and removed == 0

    def test_watchlist_fetch_exception_is_caught(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_all_watchlist_ids.return_value = {"materials": "wl1"}
        svc.robinhood.get_watchlist_items_by_id.side_effect = RuntimeError("boom")
        svc.watchlist_store.sync_watchlist.return_value = ([], [])
        added, removed = svc.sync_watchlist()
        assert added == 0 and removed == 0

    def test_fundamentals_failure_non_fatal(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_all_watchlist_ids.return_value = {"materials": "wl1"}
        svc.robinhood.get_watchlist_items_by_id.return_value = [
            {"symbol": "AAPL", "name": "Apple"},
        ]
        svc.robinhood.get_fundamentals.side_effect = RuntimeError("fund fail")
        svc.watchlist_store.sync_watchlist.return_value = ([], [])
        added, removed = svc.sync_watchlist()
        assert added == 0 and removed == 0

    def test_no_changes_skips_kafka(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_all_watchlist_ids.return_value = {"materials": "wl1"}
        svc.robinhood.get_watchlist_items_by_id.return_value = [
            {"symbol": "AAPL", "name": "Apple"},
        ]
        svc.robinhood.get_fundamentals.return_value = {}
        svc.watchlist_store.sync_watchlist.return_value = ([], [])
        svc.sync_watchlist()
        svc.kafka.publish_watchlist_update.assert_not_called()

    def test_top_level_exception_reraises(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_all_watchlist_ids.side_effect = RuntimeError("fatal")
        with pytest.raises(RuntimeError):
            svc.sync_watchlist()


# ---------------------------------------------------------------------------
# sync_earnings_calendar
# ---------------------------------------------------------------------------

class TestSyncEarningsCalendar:
    def test_not_initialized_raises(self, mock_settings):
        svc = TradeSyncService(mock_settings)
        with pytest.raises(RuntimeError, match="not initialized"):
            svc.sync_earnings_calendar()

    def test_no_symbols(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.return_value = set()
        svc.position_store.get_positions.return_value = {}
        assert svc.sync_earnings_calendar() == 0

    def test_etf_clears_earnings(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.return_value = {"SPY"}
        svc.position_store.get_positions.return_value = {}
        svc.robinhood.get_earnings.return_value = []
        assert svc.sync_earnings_calendar() == 1
        svc.earnings_store.clear_earnings.assert_called_with("SPY")

    def test_upcoming_earnings_stored(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.return_value = {"AAPL"}
        svc.position_store.get_positions.return_value = {}
        future = (date.today() + timedelta(days=10)).isoformat()
        svc.robinhood.get_earnings.return_value = [
            {
                "report": {"date": future, "timing": "am", "verified": True},
                "eps": {"actual": None},
            }
        ]
        assert svc.sync_earnings_calendar() == 1
        svc.earnings_store.store_next_earnings.assert_called_once()

    def test_already_reported_quarter_skipped(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.return_value = {"AAPL"}
        svc.position_store.get_positions.return_value = {}
        past = (date.today() - timedelta(days=10)).isoformat()
        svc.robinhood.get_earnings.return_value = [
            {"report": {"date": past}, "eps": {"actual": "1.50"}},
        ]
        svc.sync_earnings_calendar()
        svc.earnings_store.clear_earnings.assert_called_with("AAPL")

    def test_picks_nearest_upcoming(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.return_value = {"AAPL"}
        svc.position_store.get_positions.return_value = {}
        near = (date.today() + timedelta(days=5)).isoformat()
        far = (date.today() + timedelta(days=40)).isoformat()
        svc.robinhood.get_earnings.return_value = [
            {"report": {"date": far}, "eps": {"actual": None}},
            {"report": {"date": near}, "eps": {"actual": None}},
        ]
        svc.sync_earnings_calendar()
        stored = svc.earnings_store.store_next_earnings.call_args[0][1]
        assert stored["date"] == near

    def test_bad_report_date_skipped(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.return_value = {"AAPL"}
        svc.position_store.get_positions.return_value = {}
        svc.robinhood.get_earnings.return_value = [
            {"report": {"date": "not-a-date"}, "eps": {"actual": None}},
            {"report": {"date": ""}, "eps": {"actual": None}},
        ]
        svc.sync_earnings_calendar()
        svc.earnings_store.clear_earnings.assert_called_with("AAPL")

    def test_per_symbol_exception_counted(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.return_value = {"AAPL"}
        svc.position_store.get_positions.return_value = {}
        svc.robinhood.get_earnings.side_effect = RuntimeError("api fail")
        assert svc.sync_earnings_calendar() == 1

    def test_top_level_exception_returns_zero(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.side_effect = RuntimeError("fatal")
        assert svc.sync_earnings_calendar() == 0

    def test_includes_position_symbols(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.watchlist_store.get_symbols.return_value = set()
        svc.position_store.get_positions.return_value = {"TSLA": {}}
        svc.robinhood.get_earnings.return_value = []
        assert svc.sync_earnings_calendar() == 1


# ---------------------------------------------------------------------------
# sync_positions error path & reconnect
# ---------------------------------------------------------------------------

class TestSyncPositionsError:
    def test_exception_returns_false(self, mock_settings):
        svc = _wired_service(mock_settings)
        svc.robinhood.get_current_positions.side_effect = RuntimeError("rh down")
        assert svc.sync_positions() is False


class TestReconnect:
    def test_reconnect_calls_cleanup_then_initialize(self, mock_settings):
        svc = _wired_service(mock_settings)
        with patch.object(svc, "cleanup") as cl, \
             patch.object(svc, "initialize", return_value=True) as init:
            assert svc.reconnect() is True
            cl.assert_called_once()
            init.assert_called_once()
