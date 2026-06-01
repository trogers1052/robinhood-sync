"""Additional main-module tests: halt alerts, login retry strategies,
interruptible sleep, the continuous loop body, health server and main()."""

from datetime import timedelta
from unittest.mock import Mock, patch, MagicMock

import pytest

import robinhood_sync.main as main
from robinhood_sync.config import Settings
from robinhood_sync.robinhood_client import LoginOutcome

# Capture the real (unstubbed) interruptible-sleep before any autouse fixture
# replaces it, so the dedicated tests below can exercise the genuine logic.
_REAL_INTERRUPTIBLE_SLEEP = main._interruptible_sleep


@pytest.fixture(autouse=True)
def reset_shutdown():
    main._shutdown_requested = False
    yield
    main._shutdown_requested = False


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(main, "_interruptible_sleep", lambda _s: None)


@pytest.fixture
def mock_settings():
    s = Mock(spec=Settings)
    s.market_open_hour = 4
    s.market_close_hour = 20
    s.poll_interval_minutes = 10
    s.sync_history_days = 30
    s.telegram_bot_token = None
    s.telegram_chat_id = None
    s.kafka_brokers = ["b:9092"]
    s.kafka_topic = "t"
    s.kafka_positions_topic = "p"
    s.kafka_watchlist_topic = "w"
    s.redis_host = "localhost"
    s.redis_port = 6379
    return s


def _mock_service(login_outcome=LoginOutcome.SUCCESS, init=True):
    svc = Mock()
    svc.initialize.return_value = init
    svc.cleanup.return_value = None
    svc.sync_trades.return_value = (5, 2)
    svc.sync_positions.return_value = True
    svc.sync_watchlist.return_value = (1, 0)
    svc.sync_stop_orders.return_value = 3
    svc.sync_earnings_calendar.return_value = 10
    svc.is_healthy.return_value = True
    svc.reconnect.return_value = True
    svc.get_sync_stats.return_value = {"total_synced_orders": 42}
    svc.robinhood = Mock()
    svc.robinhood.last_login_outcome = login_outcome
    return svc


# ---------------------------------------------------------------------------
# _halt_with_alert
# ---------------------------------------------------------------------------

class TestHaltWithAlert:
    def test_device_challenge_message(self):
        notifier = Mock()
        main._halt_with_alert(notifier, LoginOutcome.DEVICE_CHALLENGE, "detail")
        notifier.send.assert_called_once()
        assert "device approval" in notifier.send.call_args[0][0]

    def test_bad_credentials_message(self):
        notifier = Mock()
        main._halt_with_alert(notifier, LoginOutcome.BAD_CREDENTIALS, "detail")
        assert "bad credentials" in notifier.send.call_args[0][0]

    def test_generic_halt_message(self):
        notifier = Mock()
        main._halt_with_alert(notifier, LoginOutcome.RATE_LIMITED, "detail")
        assert "halted" in notifier.send.call_args[0][0].lower()


# ---------------------------------------------------------------------------
# _interruptible_sleep
# ---------------------------------------------------------------------------

class TestInterruptibleSleep:
    def test_sleeps_in_increments(self):
        with patch("robinhood_sync.main.time.sleep") as slp:
            _REAL_INTERRUPTIBLE_SLEEP(25)
        # 10 + 10 + 5 == 3 naps
        assert slp.call_count == 3

    def test_stops_when_shutdown_requested(self):
        main._shutdown_requested = True
        with patch("robinhood_sync.main.time.sleep") as slp:
            _REAL_INTERRUPTIBLE_SLEEP(100)
        slp.assert_not_called()


# ---------------------------------------------------------------------------
# _initialize_with_retries
# ---------------------------------------------------------------------------

class TestInitializeWithRetries:
    def test_success_first_try(self):
        svc = _mock_service()
        assert main._initialize_with_retries(svc, Mock()) == LoginOutcome.SUCCESS

    def test_device_challenge_halts(self):
        svc = _mock_service(login_outcome=LoginOutcome.DEVICE_CHALLENGE, init=False)
        notifier = Mock()
        outcome = main._initialize_with_retries(svc, notifier)
        assert outcome == LoginOutcome.DEVICE_CHALLENGE
        notifier.send.assert_called_once()

    def test_login_success_but_downstream_fails_is_transient(self):
        # initialize() returns False but last_login_outcome is SUCCESS ->
        # treated as TRANSIENT, retried then exhausted.
        svc = _mock_service(login_outcome=LoginOutcome.SUCCESS, init=False)
        notifier = Mock()
        outcome = main._initialize_with_retries(svc, notifier)
        assert outcome == LoginOutcome.TRANSIENT

    def test_rate_limited_then_success(self):
        svc = _mock_service(login_outcome=LoginOutcome.RATE_LIMITED, init=False)
        # initialize fails once (rate limited), then succeeds
        svc.initialize.side_effect = [False, True]
        outcome = main._initialize_with_retries(svc, Mock())
        assert outcome == LoginOutcome.SUCCESS

    def test_rate_limited_exhausted_halts(self):
        svc = _mock_service(login_outcome=LoginOutcome.RATE_LIMITED, init=False)
        notifier = Mock()
        outcome = main._initialize_with_retries(svc, notifier)
        assert outcome == LoginOutcome.RATE_LIMITED
        notifier.send.assert_called_once()

    def test_rate_limited_shutdown_during_sleep(self):
        svc = _mock_service(login_outcome=LoginOutcome.RATE_LIMITED, init=False)

        def set_shutdown(_s):
            main._shutdown_requested = True

        with patch.object(main, "_interruptible_sleep", side_effect=set_shutdown):
            outcome = main._initialize_with_retries(svc, Mock())
        assert outcome == LoginOutcome.UNKNOWN

    def test_transient_exhausted_halts(self):
        svc = _mock_service(login_outcome=LoginOutcome.TRANSIENT, init=False)
        notifier = Mock()
        outcome = main._initialize_with_retries(svc, notifier)
        assert outcome == LoginOutcome.TRANSIENT
        notifier.send.assert_called_once()

    def test_transient_shutdown_during_sleep(self):
        svc = _mock_service(login_outcome=LoginOutcome.TRANSIENT, init=False)

        def set_shutdown(_s):
            main._shutdown_requested = True

        with patch.object(main, "_interruptible_sleep", side_effect=set_shutdown):
            outcome = main._initialize_with_retries(svc, Mock())
        assert outcome == LoginOutcome.UNKNOWN

    def test_no_robinhood_client_is_unknown(self):
        svc = _mock_service(init=False)
        svc.robinhood = None
        notifier = Mock()
        outcome = main._initialize_with_retries(svc, notifier)
        # UNKNOWN routes through transient backoff, exhausts, halts
        assert outcome == LoginOutcome.UNKNOWN


# ---------------------------------------------------------------------------
# run_continuous — loop body
# ---------------------------------------------------------------------------

class TestRunContinuousLoop:
    def _patch_env(self, svc, scheduler):
        return [
            patch.object(main, "TradeSyncService", return_value=svc),
            patch.object(main, "MarketScheduler", return_value=scheduler),
            patch.object(main, "TelegramNotifier", return_value=Mock()),
            patch.object(main, "_initialize_with_retries",
                         return_value=LoginOutcome.SUCCESS),
            patch.object(main.time, "sleep", lambda *_a, **_k: None),
        ]

    def _scheduler(self, market_open=True):
        sch = Mock()
        sch.get_sleep_duration.return_value = timedelta(seconds=0)
        sch.is_market_hours.return_value = market_open
        sch.get_status.return_value = {"day_of_week": "Mon", "next_market_open": "x"}
        sch.log_status.return_value = None
        return sch

    def test_halt_returns_zero(self, mock_settings):
        svc = _mock_service()
        sch = self._scheduler()
        patches = [
            patch.object(main, "TradeSyncService", return_value=svc),
            patch.object(main, "MarketScheduler", return_value=sch),
            patch.object(main, "TelegramNotifier", return_value=Mock()),
            patch.object(main, "_initialize_with_retries",
                         return_value=LoginOutcome.DEVICE_CHALLENGE),
        ]
        for p in patches:
            p.start()
        try:
            assert main.run_continuous(mock_settings) == 0
        finally:
            for p in patches:
                p.stop()

    def test_one_market_cycle_then_shutdown(self, mock_settings):
        svc = _mock_service()
        sch = self._scheduler(market_open=True)

        call = {"n": 0}

        def is_market_hours():
            # First the loop guard / sleep, then after sync request shutdown
            call["n"] += 1
            if call["n"] >= 3:
                main._shutdown_requested = True
            return True

        sch.is_market_hours.side_effect = is_market_hours

        patches = self._patch_env(svc, sch)
        for p in patches:
            p.start()
        try:
            rc = main.run_continuous(mock_settings)
        finally:
            for p in patches:
                p.stop()
        assert rc == 0
        svc.sync_positions.assert_called()

    def test_unhealthy_reconnect_fail_exits(self, mock_settings):
        svc = _mock_service()
        svc.is_healthy.return_value = False
        svc.reconnect.return_value = False
        sch = self._scheduler(market_open=True)
        patches = self._patch_env(svc, sch)
        for p in patches:
            p.start()
        try:
            rc = main.run_continuous(mock_settings)
        finally:
            for p in patches:
                p.stop()
        assert rc == 1

    def test_initial_sync_exception_continues_then_shutdown(self, mock_settings):
        svc = _mock_service()
        # Initial sync_trades raises; loop should still run and then exit.
        svc.sync_trades.side_effect = [RuntimeError("boom"), (1, 0), (1, 0)]
        sch = self._scheduler(market_open=False)  # market closed -> continue branch

        n = {"i": 0}

        def closed_then_shutdown():
            n["i"] += 1
            if n["i"] >= 2:
                main._shutdown_requested = True
            return False

        sch.is_market_hours.side_effect = closed_then_shutdown
        patches = self._patch_env(svc, sch)
        for p in patches:
            p.start()
        try:
            rc = main.run_continuous(mock_settings)
        finally:
            for p in patches:
                p.stop()
        assert rc == 0

    def test_sync_exception_in_loop_exits_after_max(self, mock_settings):
        svc = _mock_service()
        sch = self._scheduler(market_open=True)
        # positions sync always raises in the loop -> consecutive failures
        svc.sync_positions.side_effect = RuntimeError("sync down")
        patches = self._patch_env(svc, sch)
        for p in patches:
            p.start()
        try:
            rc = main.run_continuous(mock_settings)
        finally:
            for p in patches:
                p.stop()
        assert rc == 1


# ---------------------------------------------------------------------------
# _start_health_server & main()
# ---------------------------------------------------------------------------

class TestHealthServerAndMain:
    def test_health_server_starts_thread(self):
        with patch.object(main, "HTTPServer", return_value=Mock()), \
             patch.object(main, "threading") as thr:
            main._start_health_server()
            thr.Thread.assert_called_once()
            thr.Thread.return_value.start.assert_called_once()

    def test_main_once_mode(self, mock_settings):
        with patch.object(main, "load_dotenv"), \
             patch.object(main, "_start_health_server"), \
             patch.object(main, "start_metrics_server"), \
             patch.object(main, "get_settings", return_value=mock_settings), \
             patch.object(main, "signal"), \
             patch.object(main, "run_once", return_value=0) as ro, \
             patch("sys.argv", ["prog", "--once", "--days", "5"]):
            with pytest.raises(SystemExit) as exc:
                main.main()
            ro.assert_called_once()
        assert exc.value.code == 0

    def test_main_continuous_mode(self, mock_settings):
        with patch.object(main, "load_dotenv"), \
             patch.object(main, "_start_health_server"), \
             patch.object(main, "start_metrics_server"), \
             patch.object(main, "get_settings", return_value=mock_settings), \
             patch.object(main, "signal"), \
             patch.object(main, "run_continuous", return_value=0) as rc, \
             patch("sys.argv", ["prog"]):
            with pytest.raises(SystemExit):
                main.main()
            rc.assert_called_once()

    def test_main_debug_flag(self, mock_settings):
        with patch.object(main, "load_dotenv"), \
             patch.object(main, "_start_health_server"), \
             patch.object(main, "start_metrics_server"), \
             patch.object(main, "get_settings", return_value=mock_settings), \
             patch.object(main, "signal"), \
             patch.object(main, "run_once", return_value=0), \
             patch("sys.argv", ["prog", "--once", "--debug"]):
            with pytest.raises(SystemExit):
                main.main()

    def test_main_settings_failure_exits_1(self):
        with patch.object(main, "load_dotenv"), \
             patch.object(main, "_start_health_server"), \
             patch.object(main, "start_metrics_server"), \
             patch.object(main, "get_settings", side_effect=RuntimeError("bad cfg")), \
             patch("sys.argv", ["prog", "--once"]):
            with pytest.raises(SystemExit) as exc:
                main.main()
        assert exc.value.code == 1
