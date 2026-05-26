"""Tests for RobinhoodClient — login, instrument cache, rate limiting, API methods."""

import time
from collections import OrderedDict
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock

import pytest

from robinhood_sync.robinhood_client import (
    LoginOutcome,
    RobinhoodClient,
    Trade,
    _classify_login_failure,
)


# ---------------------------------------------------------------------------
# Constructor / state
# ---------------------------------------------------------------------------


class TestClientInit:
    def test_initial_state(self):
        c = RobinhoodClient("user", "pass", totp_secret="secret")
        assert c.username == "user"
        assert c.password == "pass"
        assert c.totp_secret == "secret"
        assert not c._logged_in
        assert isinstance(c._instrument_cache, OrderedDict)

    def test_no_totp(self):
        c = RobinhoodClient("user", "pass")
        assert c.totp_secret is None


# ---------------------------------------------------------------------------
# is_logged_in
# ---------------------------------------------------------------------------


class TestIsLoggedIn:
    def test_not_logged_in_initially(self):
        c = RobinhoodClient("user", "pass")
        assert c.is_logged_in() is False

    def test_logged_in_after_set(self):
        c = RobinhoodClient("user", "pass")
        c._logged_in = True
        assert c.is_logged_in() is True


# ---------------------------------------------------------------------------
# login
# ---------------------------------------------------------------------------


class TestLogin:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_login_success(self, mock_rh):
        mock_rh.login.return_value = {"access_token": "abc"}
        c = RobinhoodClient("user", "pass")
        result = c.login()
        assert result == LoginOutcome.SUCCESS
        assert c.last_login_outcome == LoginOutcome.SUCCESS
        assert c._logged_in is True
        # Credentials preserved for reconnection
        assert c.password == "pass"

    @patch("robinhood_sync.robinhood_client.rh")
    def test_login_returns_none_classifies_unknown(self, mock_rh):
        """rh.login() returning None with no challenge signal → UNKNOWN."""
        mock_rh.login.return_value = None
        c = RobinhoodClient("user", "pass")
        result = c.login()
        assert result == LoginOutcome.UNKNOWN
        assert c._logged_in is False

    @patch("robinhood_sync.robinhood_client.rh")
    def test_login_network_exception_classifies_transient(self, mock_rh):
        mock_rh.login.side_effect = Exception("Connection refused")
        c = RobinhoodClient("user", "pass")
        result = c.login()
        assert result == LoginOutcome.TRANSIENT
        assert c.last_login_outcome == LoginOutcome.TRANSIENT

    @patch("robinhood_sync.robinhood_client.rh")
    def test_login_429_classifies_rate_limited(self, mock_rh):
        """robin_stocks's 429 path crashes with NoneType subscript inside its own try."""
        def _fake_login(**kwargs):
            print("Starting login process...")
            print("Verification required, handling challenge...")
            print("Starting verification process...")
            print("Check robinhood app for device approvals method...")
            print("429 Client Error: Too Many Requests for url: ...")
            return None
        mock_rh.login.side_effect = _fake_login
        c = RobinhoodClient("user", "pass")
        result = c.login()
        assert result == LoginOutcome.RATE_LIMITED
        assert result.is_halt is False  # rate-limited still retryable after long cooldown

    @patch("robinhood_sync.robinhood_client.rh")
    def test_login_nonetype_subscript_classifies_rate_limited(self, mock_rh):
        """The exact TypeError pattern from robin_stocks's _validate_sherrif_id on 429."""
        mock_rh.login.side_effect = TypeError("'NoneType' object is not subscriptable")
        c = RobinhoodClient("user", "pass")
        result = c.login()
        assert result == LoginOutcome.RATE_LIMITED

    @patch("robinhood_sync.robinhood_client.rh")
    def test_login_device_challenge_classified(self, mock_rh):
        """Verification workflow detected → halt for human approval."""
        def _fake_login(**kwargs):
            print("Starting login process...")
            print("Verification required, handling challenge...")
            print("Starting verification process...")
            print("Check robinhood app for device approvals method...")
            return None
        mock_rh.login.side_effect = _fake_login
        c = RobinhoodClient("user", "pass")
        result = c.login()
        assert result == LoginOutcome.DEVICE_CHALLENGE
        assert result.is_halt is True

    @patch("robinhood_sync.robinhood_client.rh")
    def test_login_bad_credentials_classified(self, mock_rh):
        def _fake_login(**kwargs):
            print("Login failed. Check credentials and try again.")
            return None
        mock_rh.login.side_effect = _fake_login
        c = RobinhoodClient("user", "pass")
        result = c.login()
        assert result == LoginOutcome.BAD_CREDENTIALS
        assert result.is_halt is True

    @patch("robinhood_sync.robinhood_client.rh")
    def test_login_with_totp(self, mock_rh):
        """Login with TOTP generates mfa_code and passes it to rh.login."""
        mock_rh.login.return_value = {"access_token": "abc"}
        c = RobinhoodClient("user", "pass", totp_secret="JBSWY3DPEHPK3PXP")

        # pyotp is imported inside login(), so we patch it at the module level
        import sys
        mock_pyotp = Mock()
        mock_totp_obj = Mock()
        mock_totp_obj.now.return_value = "123456"
        mock_pyotp.TOTP.return_value = mock_totp_obj

        with patch.dict(sys.modules, {"pyotp": mock_pyotp}):
            result = c.login()

        assert result == LoginOutcome.SUCCESS
        # Verify TOTP code was passed
        _, kwargs = mock_rh.login.call_args
        assert kwargs["mfa_code"] == "123456"


class TestClassifier:
    """Unit tests for _classify_login_failure with synthetic inputs."""

    def test_rate_limit_dominates_challenge(self):
        # Even when both 429 and challenge markers appear, classify as RATE_LIMITED.
        stdout = (
            "Verification required, handling challenge...\n"
            "Check robinhood app for device approvals method...\n"
            "429 Client Error: Too Many Requests for url: .../get_prompts_status/\n"
        )
        assert _classify_login_failure(stdout, None) == LoginOutcome.RATE_LIMITED

    def test_pure_device_challenge(self):
        stdout = (
            "Verification required, handling challenge...\n"
            "Starting verification process...\n"
            "Check robinhood app for device approvals method...\n"
        )
        assert _classify_login_failure(stdout, None) == LoginOutcome.DEVICE_CHALLENGE

    def test_bad_credentials_signal(self):
        stdout = "Login failed. Check credentials and try again.\n"
        assert _classify_login_failure(stdout, None) == LoginOutcome.BAD_CREDENTIALS

    def test_transient_from_exception(self):
        exc = ConnectionError("Connection reset by peer")
        assert _classify_login_failure("", exc) == LoginOutcome.TRANSIENT

    def test_unknown_when_silent(self):
        # rh.login returning None silently with no stdout and no exception
        # is unclassifiable — surface as UNKNOWN.
        assert _classify_login_failure("", None) == LoginOutcome.UNKNOWN

    def test_nonetype_subscript_from_stdout(self):
        # Sometimes robin_stocks prints the error instead of raising.
        stdout = "Error during login verification: 'NoneType' object is not subscriptable\n"
        assert _classify_login_failure(stdout, None) == LoginOutcome.RATE_LIMITED


# ---------------------------------------------------------------------------
# logout
# ---------------------------------------------------------------------------


class TestLogout:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_logout(self, mock_rh):
        c = RobinhoodClient("user", "pass")
        c._logged_in = True
        c.logout()
        assert c._logged_in is False
        mock_rh.logout.assert_called_once()

    @patch("robinhood_sync.robinhood_client.rh")
    def test_logout_exception(self, mock_rh):
        mock_rh.logout.side_effect = Exception("error")
        c = RobinhoodClient("user", "pass")
        c._logged_in = True
        c.logout()  # Should not raise


# ---------------------------------------------------------------------------
# _get_symbol_from_instrument (cache)
# ---------------------------------------------------------------------------


class TestInstrumentCache:
    def test_cache_hit(self):
        c = RobinhoodClient("user", "pass")
        c._instrument_cache["http://example.com/1/"] = "AAPL"
        result = c._get_symbol_from_instrument("http://example.com/1/")
        assert result == "AAPL"

    @patch("robinhood_sync.robinhood_client.rh")
    def test_cache_miss_fetches(self, mock_rh):
        mock_rh.stocks.get_instrument_by_url.return_value = {"symbol": "GOOG"}
        c = RobinhoodClient("user", "pass")
        c._last_api_call = 0  # skip rate limit
        result = c._get_symbol_from_instrument("http://example.com/2/")
        assert result == "GOOG"
        assert "http://example.com/2/" in c._instrument_cache

    @patch("robinhood_sync.robinhood_client.rh")
    def test_cache_miss_unknown_symbol(self, mock_rh):
        mock_rh.stocks.get_instrument_by_url.return_value = {}
        c = RobinhoodClient("user", "pass")
        c._last_api_call = 0
        result = c._get_symbol_from_instrument("http://example.com/3/")
        assert result == "UNKNOWN"

    @patch("robinhood_sync.robinhood_client.rh")
    def test_cache_eviction(self, mock_rh):
        c = RobinhoodClient("user", "pass")
        c._instrument_cache_max = 2
        c._last_api_call = 0

        mock_rh.stocks.get_instrument_by_url.side_effect = [
            {"symbol": "A"},
            {"symbol": "B"},
            {"symbol": "C"},
        ]

        c._get_symbol_from_instrument("url_a")
        c._get_symbol_from_instrument("url_b")
        c._get_symbol_from_instrument("url_c")

        # Cache should only have 2 entries, oldest evicted
        assert len(c._instrument_cache) == 2
        assert "url_a" not in c._instrument_cache

    @patch("robinhood_sync.robinhood_client.rh")
    def test_cache_exception_returns_unknown(self, mock_rh):
        mock_rh.stocks.get_instrument_by_url.side_effect = Exception("network")
        c = RobinhoodClient("user", "pass")
        c._last_api_call = 0
        result = c._get_symbol_from_instrument("http://fail/")
        assert result == "UNKNOWN"

    def test_cache_move_to_end_on_hit(self):
        c = RobinhoodClient("user", "pass")
        c._instrument_cache["url_1"] = "A"
        c._instrument_cache["url_2"] = "B"
        # Access url_1 to move to end
        c._get_symbol_from_instrument("url_1")
        # url_2 should now be first (oldest)
        first = next(iter(c._instrument_cache))
        assert first == "url_2"


# ---------------------------------------------------------------------------
# get_all_orders — requires login
# ---------------------------------------------------------------------------


class TestGetAllOrders:
    def test_not_logged_in_raises(self):
        c = RobinhoodClient("user", "pass")
        with pytest.raises(RuntimeError, match="Not logged in"):
            c.get_all_orders()

    @patch("robinhood_sync.robinhood_client.rh")
    def test_empty_orders(self, mock_rh):
        mock_rh.orders.get_all_stock_orders.return_value = []
        c = RobinhoodClient("user", "pass")
        c._logged_in = True
        c._last_api_call = 0
        result = c.get_all_orders()
        assert result == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_none_orders(self, mock_rh):
        mock_rh.orders.get_all_stock_orders.return_value = None
        c = RobinhoodClient("user", "pass")
        c._logged_in = True
        c._last_api_call = 0
        result = c.get_all_orders()
        assert result == []


# ---------------------------------------------------------------------------
# get_current_positions — requires login
# ---------------------------------------------------------------------------


class TestGetCurrentPositions:
    def test_not_logged_in_raises(self):
        c = RobinhoodClient("user", "pass")
        with pytest.raises(RuntimeError, match="Not logged in"):
            c.get_current_positions()


# ---------------------------------------------------------------------------
# get_account_balance — requires login
# ---------------------------------------------------------------------------


class TestGetAccountBalance:
    def test_not_logged_in_raises(self):
        c = RobinhoodClient("user", "pass")
        with pytest.raises(RuntimeError, match="Not logged in"):
            c.get_account_balance()


# ---------------------------------------------------------------------------
# get_watchlist — requires login
# ---------------------------------------------------------------------------


class TestGetWatchlist:
    def test_not_logged_in_raises(self):
        c = RobinhoodClient("user", "pass")
        with pytest.raises(RuntimeError, match="Not logged in"):
            c.get_watchlist()


# ---------------------------------------------------------------------------
# get_stop_orders — requires login
# ---------------------------------------------------------------------------


class TestGetStopOrders:
    def test_not_logged_in_raises(self):
        c = RobinhoodClient("user", "pass")
        with pytest.raises(RuntimeError, match="Not logged in"):
            c.get_stop_orders()


# ---------------------------------------------------------------------------
# get_all_watchlist_symbols — requires login
# ---------------------------------------------------------------------------


class TestGetAllWatchlistSymbols:
    def test_not_logged_in_raises(self):
        c = RobinhoodClient("user", "pass")
        with pytest.raises(RuntimeError, match="Not logged in"):
            c.get_all_watchlist_symbols()


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


class TestRateLimit:
    def test_rate_limit_sleeps_when_too_fast(self):
        c = RobinhoodClient("user", "pass")
        c._last_api_call = time.monotonic()  # just called
        with patch("robinhood_sync.robinhood_client.time.sleep") as mock_sleep:
            c._rate_limit()
            # Should have slept some amount
            mock_sleep.assert_called_once()
            sleep_time = mock_sleep.call_args[0][0]
            assert 0 < sleep_time <= 1.0

    def test_rate_limit_no_sleep_when_enough_time(self):
        c = RobinhoodClient("user", "pass")
        c._last_api_call = time.monotonic() - 2.0  # 2 seconds ago
        with patch("robinhood_sync.robinhood_client.time.sleep") as mock_sleep:
            c._rate_limit()
            mock_sleep.assert_not_called()
