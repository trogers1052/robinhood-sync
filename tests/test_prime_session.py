"""
Tests for the interactive session primer.

The primer is the human-present recovery path, so what matters is that it
never silently does the wrong thing: it must not log in when a session can be
resumed, must surface failure outcomes as non-zero exits, and must respect
--no-redis so a laptop run can't be forced through an unreachable Pi.
"""

from unittest.mock import Mock, patch

import pytest

from robinhood_sync import prime_session
from robinhood_sync.session import LoginOutcome


@pytest.fixture
def manager():
    m = Mock()
    m.device_token = "device-1"
    m.bundle = Mock(expires_at=1_800_000_000.0, refresh_token="r")
    return m


@pytest.fixture
def settings():
    s = Mock()
    s.robinhood_username = "trader@example.com"
    s.robinhood_session_file = "/tmp/session.json"
    s.redis_host = "127.0.0.1"
    s.redis_port = 6379
    return s


def _run(argv, manager, settings):
    with patch.object(prime_session, "get_settings", return_value=settings), \
         patch.object(
             prime_session.SessionManager, "from_settings", return_value=manager
         ):
        return prime_session.main(argv)


class TestPrime:
    def test_successful_login_exits_zero(self, manager, settings):
        manager.password_login.return_value = LoginOutcome.SUCCESS
        assert _run([], manager, settings) == 0
        manager.password_login.assert_called_once()

    @pytest.mark.parametrize(
        "outcome",
        [
            LoginOutcome.RATE_LIMITED,
            LoginOutcome.BAD_CREDENTIALS,
            LoginOutcome.DEVICE_CHALLENGE,
            LoginOutcome.UNKNOWN,
        ],
    )
    def test_failed_login_exits_nonzero(self, outcome, manager, settings):
        manager.password_login.return_value = outcome
        assert _run([], manager, settings) == 1

    def test_reuse_skips_login_when_session_resumes(self, manager, settings):
        """The cheapest recovery: no login means no possible device challenge."""
        manager.authenticate.return_value = LoginOutcome.SUCCESS
        assert _run(["--reuse"], manager, settings) == 0
        manager.password_login.assert_not_called()

    def test_reuse_falls_through_to_login_when_resume_fails(self, manager, settings):
        manager.authenticate.return_value = LoginOutcome.DEVICE_CHALLENGE
        manager.password_login.return_value = LoginOutcome.SUCCESS
        assert _run(["--reuse"], manager, settings) == 0
        manager.password_login.assert_called_once()

    def test_force_ignores_reuse(self, manager, settings):
        manager.password_login.return_value = LoginOutcome.SUCCESS
        assert _run(["--reuse", "--force"], manager, settings) == 0
        manager.authenticate.assert_not_called()

    def test_no_redis_is_passed_through(self, manager, settings):
        manager.password_login.return_value = LoginOutcome.SUCCESS
        with patch.object(prime_session, "get_settings", return_value=settings), \
             patch.object(
                 prime_session.SessionManager, "from_settings", return_value=manager
             ) as from_settings:
            prime_session.main(["--no-redis"])
        assert from_settings.call_args.kwargs["redis_enabled"] is False
        assert from_settings.call_args.kwargs["interactive"] is True

    def test_device_token_override_reaches_settings(self, manager, settings):
        manager.password_login.return_value = LoginOutcome.SUCCESS
        _run(["--device-token", "pinned"], manager, settings)
        assert settings.robinhood_device_token == "pinned"

    def test_import_pickle_short_circuits_login(self, manager, settings):
        manager.import_legacy_pickle.return_value = True
        manager.ensure_fresh.return_value = True
        assert _run(["--import-pickle", "/tmp/robinhood.pickle"], manager, settings) == 0
        manager.password_login.assert_not_called()

    def test_unrefreshable_pickle_falls_back_to_login(self, manager, settings):
        manager.import_legacy_pickle.return_value = True
        manager.ensure_fresh.return_value = False
        manager.password_login.return_value = LoginOutcome.SUCCESS
        assert _run(["--import-pickle", "/tmp/robinhood.pickle"], manager, settings) == 0
        manager.password_login.assert_called_once()

    def test_missing_pickle_falls_back_to_login(self, manager, settings):
        manager.import_legacy_pickle.return_value = False
        manager.password_login.return_value = LoginOutcome.SUCCESS
        assert _run(["--import-pickle", "/nope.pickle"], manager, settings) == 0
        manager.password_login.assert_called_once()

    def test_keyboard_interrupt_exits_130(self, manager, settings):
        with patch.object(prime_session, "prime", side_effect=KeyboardInterrupt):
            assert prime_session.main([]) == 130

    def test_report_handles_missing_bundle(self, manager, settings):
        """A manager that succeeded but exposes no bundle must not crash the report."""
        manager.bundle = None
        manager.password_login.return_value = LoginOutcome.SUCCESS
        assert _run([], manager, settings) == 0


class TestSettingsLoading:
    def test_prompts_when_settings_incomplete(self, manager):
        built = Mock()
        with patch.object(
            prime_session, "get_settings", side_effect=ValueError("missing password")
        ), patch.object(prime_session, "Settings", return_value=built) as settings_cls, \
             patch("builtins.input", return_value="trader@example.com"), \
             patch.object(prime_session.getpass, "getpass", return_value="hunter2"), \
             patch.object(
                 prime_session.SessionManager, "from_settings", return_value=manager
             ):
            manager.password_login.return_value = LoginOutcome.SUCCESS
            assert prime_session.main([]) == 0
        assert settings_cls.call_args.kwargs["robinhood_username"] == "trader@example.com"

    def test_username_flag_skips_the_prompt(self, manager):
        with patch.object(
            prime_session, "get_settings", side_effect=ValueError("missing password")
        ), patch.object(prime_session, "Settings", return_value=Mock()), \
             patch("builtins.input", side_effect=AssertionError("should not prompt")), \
             patch.object(prime_session.getpass, "getpass", return_value="hunter2"), \
             patch.object(
                 prime_session.SessionManager, "from_settings", return_value=manager
             ):
            manager.password_login.return_value = LoginOutcome.SUCCESS
            assert prime_session.main(["--username", "trader@example.com"]) == 0

    def test_empty_credentials_abort(self):
        with patch.object(
            prime_session, "get_settings", side_effect=ValueError("missing")
        ), patch("builtins.input", return_value=""), \
             patch.object(prime_session.getpass, "getpass", return_value=""):
            assert prime_session.main([]) == 1

    def test_unbuildable_settings_abort(self):
        with patch.object(
            prime_session, "get_settings", side_effect=ValueError("missing")
        ), patch.object(prime_session, "Settings", side_effect=ValueError("bad")), \
             patch("builtins.input", return_value="u"), \
             patch.object(prime_session.getpass, "getpass", return_value="p"):
            assert prime_session.main([]) == 1
