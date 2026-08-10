"""
Tests for the owned session lifecycle: bundle math, storage backends, and the
resume → refresh → password-login ordering that keeps the Pi off the device
challenge path.
"""

import copy
import json
import os
import stat
import time
from unittest.mock import Mock, patch

import pytest
import robin_stocks.robinhood.authentication as _rs_auth
from robin_stocks.robinhood.globals import SESSION as real_session

from robinhood_sync import session as session_mod
from robinhood_sync.session import (
    CLIENT_ID,
    FileSessionBackend,
    LoginOutcome,
    RedisSessionBackend,
    SessionBundle,
    SessionDocument,
    SessionManager,
    SessionStore,
    classify_login_failure,
    generate_device_token,
    pin_device_token,
)


@pytest.fixture(autouse=True)
def restore_device_token_generator():
    """pin_device_token() mutates robin_stocks globals — undo it per test."""
    original = _rs_auth.generate_device_token
    yield
    _rs_auth.generate_device_token = original


@pytest.fixture(autouse=True)
def isolate_robin_stocks_session_state():
    """Keep hydrate()/logout() from leaking login state across tests."""
    yield
    session_mod.set_login_state(False)
    session_mod.update_session("Authorization", None)


class MemoryBackend:
    """In-memory session backend for exercising SessionStore/SessionManager."""

    name = "memory"

    def __init__(self, data=None, fail_save=False, fail_load=False):
        self.data = data
        self.fail_save = fail_save
        self.fail_load = fail_load
        self.saves = 0

    def load(self):
        if self.fail_load:
            raise RuntimeError("backend exploded")
        return copy.deepcopy(self.data)

    def save(self, data):
        self.saves += 1
        if self.fail_save:
            return False
        self.data = copy.deepcopy(data)
        return True

    def clear(self):
        self.data = None


def _response(status_code=200, json_body=None, text=""):
    res = Mock()
    res.status_code = status_code
    res.text = text
    if json_body is None:
        res.json.side_effect = ValueError("no json")
    else:
        res.json.return_value = json_body
    return res


def _bundle(**overrides):
    now = time.time()
    defaults = dict(
        access_token="access-1",
        refresh_token="refresh-1",
        token_type="Bearer",
        device_token="device-1",
        issued_at=now,
        expires_at=now + 86400,
        username="trader@example.com",
    )
    defaults.update(overrides)
    return SessionBundle(**defaults)


def _manager(store=None, **kwargs):
    kwargs.setdefault("device_token", "device-1")
    return SessionManager(
        username="trader@example.com",
        password="hunter2",
        store=store if store is not None else SessionStore([MemoryBackend()]),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# SessionBundle
# ---------------------------------------------------------------------------


class TestSessionBundle:
    def test_from_token_response(self):
        b = SessionBundle.from_token_response(
            {
                "access_token": "a",
                "refresh_token": "r",
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": "internal",
            },
            device_token="dev",
            username="u",
            now=1000.0,
        )
        assert (b.access_token, b.refresh_token, b.device_token) == ("a", "r", "dev")
        assert b.issued_at == 1000.0
        assert b.expires_at == 4600.0

    def test_from_token_response_missing_expires_in_uses_default(self):
        b = SessionBundle.from_token_response(
            {"access_token": "a"}, device_token="dev", now=0.0, default_expires_in=100
        )
        assert b.expires_at == 100.0

    def test_from_token_response_garbage_expires_in_uses_default(self):
        b = SessionBundle.from_token_response(
            {"access_token": "a", "expires_in": "soon"},
            device_token="dev",
            now=0.0,
            default_expires_in=50,
        )
        assert b.expires_at == 50.0

    def test_from_dict_roundtrip(self):
        b = _bundle()
        assert SessionBundle.from_dict(b.to_dict()) == b

    @pytest.mark.parametrize("bad", [None, {}, {"refresh_token": "r"}, "not-a-dict"])
    def test_from_dict_rejects_unusable(self, bad):
        assert SessionBundle.from_dict(bad) is None

    def test_from_dict_survives_bad_types(self):
        assert SessionBundle.from_dict(
            {"access_token": "a", "issued_at": "yesterday"}
        ) is None

    def test_seconds_remaining_and_expiry(self):
        b = _bundle(issued_at=0.0, expires_at=100.0)
        assert b.seconds_remaining(now=40.0) == 60.0
        assert b.is_expired(now=40.0) is False
        assert b.is_expired(now=100.0) is True
        assert b.is_expired(now=95.0, margin_sec=10.0) is True

    def test_needs_refresh_at_half_life(self):
        b = _bundle(issued_at=0.0, expires_at=1000.0)
        assert b.needs_refresh(now=400.0, ratio=0.5, margin_sec=0) is False
        assert b.needs_refresh(now=500.0, ratio=0.5, margin_sec=0) is True

    def test_needs_refresh_honours_margin(self):
        b = _bundle(issued_at=0.0, expires_at=1000.0)
        assert b.needs_refresh(now=100.0, ratio=0.99, margin_sec=950.0) is True

    def test_needs_refresh_when_lifetime_unknown(self):
        """An imported pickle has no known lifetime → refresh immediately."""
        b = _bundle(issued_at=500.0, expires_at=500.0)
        assert b.needs_refresh(now=500.0) is True

    def test_authorization_header(self):
        assert _bundle(token_type="Bearer", access_token="tok").authorization_header() == (
            "Bearer tok"
        )

    def test_redacted_never_leaks_tokens(self):
        b = _bundle(access_token="SECRET-ACCESS", refresh_token="SECRET-REFRESH")
        text = b.redacted()
        assert "SECRET-ACCESS" not in text
        assert "SECRET-REFRESH" not in text
        assert "refreshable=yes" in text

    def test_redacted_flags_missing_refresh_token(self):
        assert "refreshable=NO" in _bundle(refresh_token="").redacted()


# ---------------------------------------------------------------------------
# SessionDocument
# ---------------------------------------------------------------------------


class TestSessionDocument:
    def test_roundtrip(self):
        doc = SessionDocument(device_token="dev", bundle=_bundle())
        restored = SessionDocument.from_dict(doc.to_dict())
        assert restored.device_token == "dev"
        assert restored.bundle == doc.bundle

    def test_device_token_falls_back_to_bundle(self):
        doc = SessionDocument.from_dict(
            {"session": _bundle(device_token="from-bundle").to_dict()}
        )
        assert doc.device_token == "from-bundle"

    def test_device_token_only_document(self):
        doc = SessionDocument.from_dict({"device_token": "dev", "session": None})
        assert doc.device_token == "dev"
        assert doc.bundle is None

    @pytest.mark.parametrize("bad", [None, "nope", 42])
    def test_from_dict_tolerates_garbage(self, bad):
        doc = SessionDocument.from_dict(bad)
        assert doc.device_token is None and doc.bundle is None

    def test_freshness_prefers_bundle_issue_time(self):
        doc = SessionDocument(device_token="d", bundle=_bundle(issued_at=900.0))
        doc.updated_at = 100.0
        assert doc.freshness == 900.0


# ---------------------------------------------------------------------------
# FileSessionBackend
# ---------------------------------------------------------------------------


class TestFileSessionBackend:
    def test_roundtrip(self, tmp_path):
        backend = FileSessionBackend(str(tmp_path / "sub" / "session.json"))
        doc = SessionDocument(device_token="dev", bundle=_bundle()).to_dict()
        assert backend.save(doc) is True
        assert backend.load() == doc

    def test_written_private_and_atomically(self, tmp_path):
        path = tmp_path / "session.json"
        backend = FileSessionBackend(str(path))
        backend.save({"device_token": "dev"})
        mode = stat.S_IMODE(os.stat(path).st_mode)
        assert mode == 0o600
        # No temp file left behind.
        assert list(tmp_path.iterdir()) == [path]

    def test_missing_file_returns_none(self, tmp_path):
        assert FileSessionBackend(str(tmp_path / "nope.json")).load() is None

    def test_corrupt_json_returns_none(self, tmp_path):
        path = tmp_path / "session.json"
        path.write_text("{not json")
        assert FileSessionBackend(str(path)).load() is None

    def test_save_failure_is_reported_not_raised(self, tmp_path):
        backend = FileSessionBackend(str(tmp_path / "session.json"))
        with patch("robinhood_sync.session.json.dump", side_effect=OSError("disk full")):
            assert backend.save({"device_token": "d"}) is False

    def test_expands_user_path(self):
        backend = FileSessionBackend("~/.tokens/session.json")
        assert "~" not in str(backend.path)

    def test_clear_removes_file(self, tmp_path):
        path = tmp_path / "session.json"
        backend = FileSessionBackend(str(path))
        backend.save({"device_token": "d"})
        backend.clear()
        assert not path.exists()
        backend.clear()  # idempotent


# ---------------------------------------------------------------------------
# RedisSessionBackend
# ---------------------------------------------------------------------------


@pytest.fixture
def redis_settings():
    s = Mock()
    s.redis_host = "localhost"
    s.redis_port = 6379
    s.redis_db = 0
    s.redis_password = None
    s.redis_session_key = "robinhood:session"
    return s


class TestRedisSessionBackend:
    def _backend(self, settings, client):
        backend = RedisSessionBackend(settings)
        backend._client = client
        return backend

    def test_save_and_load(self, redis_settings):
        client = Mock()
        backend = self._backend(redis_settings, client)
        doc = {"device_token": "dev", "session": None}
        assert backend.save(doc) is True
        key, payload = client.set.call_args[0]
        assert key == "robinhood:session"
        client.get.return_value = payload
        assert backend.load() == doc

    def test_save_has_no_ttl(self, redis_settings):
        """The device token must outlive the tokens it was stored with."""
        client = Mock()
        self._backend(redis_settings, client).save({"device_token": "dev"})
        assert client.set.call_args.kwargs == {}
        assert len(client.set.call_args[0]) == 2

    def test_load_missing_key(self, redis_settings):
        client = Mock()
        client.get.return_value = None
        assert self._backend(redis_settings, client).load() is None

    def test_load_corrupt_json(self, redis_settings):
        client = Mock()
        client.get.return_value = "{not json"
        assert self._backend(redis_settings, client).load() is None

    def test_redis_error_on_load_is_swallowed(self, redis_settings):
        import redis as redis_lib

        client = Mock()
        client.get.side_effect = redis_lib.RedisError("down")
        assert self._backend(redis_settings, client).load() is None

    def test_redis_error_on_save_is_swallowed(self, redis_settings):
        import redis as redis_lib

        client = Mock()
        client.set.side_effect = redis_lib.RedisError("down")
        assert self._backend(redis_settings, client).save({"a": 1}) is False

    def test_connect_failure_returns_false(self, redis_settings):
        import redis as redis_lib

        backend = RedisSessionBackend(redis_settings)
        with patch.object(
            backend, "_create_client", side_effect=redis_lib.RedisError("refused")
        ):
            assert backend.connect() is False
        assert backend._client is None

    def test_load_without_client_attempts_connect(self, redis_settings):
        backend = RedisSessionBackend(redis_settings)
        backend._client = None
        with patch.object(backend, "connect", return_value=False) as conn:
            assert backend.load() is None
        conn.assert_called_once()

    def test_clear_deletes_key(self, redis_settings):
        client = Mock()
        self._backend(redis_settings, client).clear()
        client.delete.assert_called_once_with("robinhood:session")


# ---------------------------------------------------------------------------
# SessionStore
# ---------------------------------------------------------------------------


class TestSessionStore:
    def test_newest_backend_wins(self):
        """A session primed into Redis beats a stale local file mirror."""
        stale = MemoryBackend(
            SessionDocument(device_token="dev", bundle=_bundle(issued_at=100.0)).to_dict()
        )
        fresh_doc = SessionDocument(
            device_token="dev", bundle=_bundle(issued_at=900.0, access_token="newer")
        ).to_dict()
        fresh = MemoryBackend(fresh_doc)
        loaded = SessionStore([stale, fresh]).load()
        assert loaded.bundle.access_token == "newer"

    def test_load_with_no_data_returns_empty_document(self):
        doc = SessionStore([MemoryBackend()]).load()
        assert doc.device_token is None and doc.bundle is None

    def test_load_survives_exploding_backend(self):
        good = MemoryBackend(SessionDocument(device_token="dev").to_dict())
        store = SessionStore([MemoryBackend(fail_load=True), good])
        assert store.load().device_token == "dev"

    def test_save_writes_every_backend(self):
        a, b = MemoryBackend(), MemoryBackend()
        assert SessionStore([a, b]).save(SessionDocument(device_token="dev")) == 2
        assert a.data["device_token"] == "dev"
        assert b.data["device_token"] == "dev"

    def test_save_counts_only_successes(self):
        ok, bad = MemoryBackend(), MemoryBackend(fail_save=True)
        assert SessionStore([ok, bad]).save(SessionDocument(device_token="d")) == 1

    def test_save_stamps_updated_at(self):
        backend = MemoryBackend()
        doc = SessionDocument(device_token="d", updated_at=0.0)
        SessionStore([backend]).save(doc)
        assert backend.data["updated_at"] > 0

    def test_none_backends_are_dropped(self):
        assert SessionStore([None, MemoryBackend()]).backends.__len__() == 1

    def test_clear_and_close_are_defensive(self):
        backend = MemoryBackend()
        backend.close = Mock(side_effect=RuntimeError("nope"))
        store = SessionStore([backend])
        store.clear()
        store.close()  # must not raise
        assert backend.data is None


# ---------------------------------------------------------------------------
# Device token pinning
# ---------------------------------------------------------------------------


class TestDeviceToken:
    def test_pin_overrides_robin_stocks_generator(self):
        pin_device_token("pinned-token")
        assert _rs_auth.generate_device_token() == "pinned-token"

    def test_generate_uses_robinhood_format(self):
        token = generate_device_token()
        assert len(token) == 36 and token.count("-") == 4

    def test_configured_token_wins(self):
        mgr = _manager(device_token="configured")
        assert mgr.device_token == "configured"

    def test_persisted_token_used_when_unconfigured(self):
        backend = MemoryBackend(SessionDocument(device_token="persisted").to_dict())
        mgr = _manager(store=SessionStore([backend]), device_token=None)
        mgr.authenticate()  # resolves the device token
        assert mgr.device_token == "persisted"

    def test_generated_token_is_persisted_immediately(self):
        """A generated token must survive even if the login that follows fails."""
        backend = MemoryBackend()
        mgr = _manager(store=SessionStore([backend]), device_token=None)
        with patch.object(mgr, "password_login", return_value=LoginOutcome.UNKNOWN):
            mgr.authenticate()
        assert backend.data["device_token"] == mgr.device_token
        assert backend.data["device_token"]

    def test_resolution_pins_robin_stocks(self):
        mgr = _manager(device_token="configured")
        with patch.object(mgr, "password_login", return_value=LoginOutcome.UNKNOWN):
            mgr.authenticate()
        assert _rs_auth.generate_device_token() == "configured"


# ---------------------------------------------------------------------------
# authenticate() ordering
# ---------------------------------------------------------------------------


class TestAuthenticate:
    def test_resumes_valid_session_without_logging_in(self):
        backend = MemoryBackend(
            SessionDocument(device_token="device-1", bundle=_bundle()).to_dict()
        )
        mgr = _manager(store=SessionStore([backend]))
        fake = Mock()
        fake.get.return_value = _response(200, {})
        with patch.object(session_mod, "SESSION", fake), \
             patch.object(mgr, "password_login") as login:
            assert mgr.authenticate() == LoginOutcome.SUCCESS
        login.assert_not_called()
        fake.post.assert_not_called()  # no refresh needed either

    def test_refreshes_when_past_half_life(self):
        now = time.time()
        aged = _bundle(issued_at=now - 50_000, expires_at=now + 36_400)
        backend = MemoryBackend(
            SessionDocument(device_token="device-1", bundle=aged).to_dict()
        )
        mgr = _manager(store=SessionStore([backend]))
        fake = Mock()
        fake.post.return_value = _response(
            200, {"access_token": "fresh", "refresh_token": "rotated", "expires_in": 86400}
        )
        fake.get.return_value = _response(200, {})
        with patch.object(session_mod, "SESSION", fake), \
             patch.object(mgr, "password_login") as login:
            assert mgr.authenticate() == LoginOutcome.SUCCESS
        login.assert_not_called()
        assert mgr.bundle.access_token == "fresh"

    def test_rejected_access_token_forces_refresh_then_succeeds(self):
        backend = MemoryBackend(
            SessionDocument(device_token="device-1", bundle=_bundle()).to_dict()
        )
        mgr = _manager(store=SessionStore([backend]))
        fake = Mock()
        fake.get.side_effect = [_response(401), _response(200, {})]
        fake.post.return_value = _response(
            200, {"access_token": "fresh", "refresh_token": "rotated"}
        )
        with patch.object(session_mod, "SESSION", fake), \
             patch.object(mgr, "password_login") as login:
            assert mgr.authenticate() == LoginOutcome.SUCCESS
        login.assert_not_called()
        assert mgr.bundle.access_token == "fresh"

    def test_falls_back_to_password_login_without_a_bundle(self):
        mgr = _manager(store=SessionStore([MemoryBackend()]))
        with patch.object(mgr, "password_login", return_value=LoginOutcome.SUCCESS) as login:
            assert mgr.authenticate() == LoginOutcome.SUCCESS
        login.assert_called_once()

    def test_rate_limited_refresh_never_escalates_to_login(self):
        """The crash-loop guard: a 429 must not trigger a password login."""
        now = time.time()
        aged = _bundle(issued_at=now - 90_000, expires_at=now - 1)
        backend = MemoryBackend(
            SessionDocument(device_token="device-1", bundle=aged).to_dict()
        )
        mgr = _manager(store=SessionStore([backend]))
        fake = Mock()
        fake.post.return_value = _response(429, text="Too Many Requests")
        with patch.object(session_mod, "SESSION", fake), \
             patch.object(mgr, "password_login") as login:
            assert mgr.authenticate() == LoginOutcome.RATE_LIMITED
        login.assert_not_called()

    def test_password_login_can_be_disabled(self):
        mgr = _manager(store=SessionStore([MemoryBackend()]), allow_password_login=False)
        with patch.object(mgr, "password_login") as login:
            assert mgr.authenticate() == LoginOutcome.DEVICE_CHALLENGE
        login.assert_not_called()

    def test_outcome_is_recorded(self):
        mgr = _manager()
        with patch.object(mgr, "password_login", return_value=LoginOutcome.BAD_CREDENTIALS):
            mgr.authenticate()
        assert mgr.last_outcome == LoginOutcome.BAD_CREDENTIALS


# ---------------------------------------------------------------------------
# _refresh
# ---------------------------------------------------------------------------


class TestRefresh:
    def test_sends_refresh_grant(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(
            200, {"access_token": "a2", "refresh_token": "r2", "expires_in": 3600}
        )
        with patch.object(session_mod, "SESSION", fake):
            mgr._refresh(_bundle())
        url, = fake.post.call_args[0]
        payload = fake.post.call_args.kwargs["data"]
        assert url == session_mod.TOKEN_URL
        assert payload["grant_type"] == "refresh_token"
        assert payload["refresh_token"] == "refresh-1"
        assert payload["client_id"] == CLIENT_ID
        assert payload["device_token"] == "device-1"

    def test_rotated_refresh_token_is_persisted_immediately(self):
        """Losing a rotated refresh token breaks the chain — persist first."""
        backend = MemoryBackend()
        mgr = _manager(store=SessionStore([backend]))
        fake = Mock()
        fake.post.return_value = _response(
            200, {"access_token": "a2", "refresh_token": "rotated"}
        )
        with patch.object(session_mod, "SESSION", fake):
            mgr._refresh(_bundle())
        assert backend.data["session"]["refresh_token"] == "rotated"

    def test_keeps_old_refresh_token_when_response_omits_one(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(200, {"access_token": "a2"})
        with patch.object(session_mod, "SESSION", fake):
            refreshed = mgr._refresh(_bundle(refresh_token="original"))
        assert refreshed.refresh_token == "original"

    def test_without_refresh_token_returns_none(self):
        mgr = _manager()
        assert mgr._refresh(_bundle(refresh_token="")) is None

    def test_429_sets_rate_limited_flag(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(429)
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._refresh(_bundle()) is None
        assert mgr._rate_limited is True

    @pytest.mark.parametrize("status", [400, 401, 500])
    def test_non_200_returns_none(self, status):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(status, text="nope")
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._refresh(_bundle()) is None
        assert mgr._rate_limited is False

    def test_network_error_returns_none(self):
        mgr = _manager()
        fake = Mock()
        fake.post.side_effect = OSError("connection reset")
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._refresh(_bundle()) is None

    def test_non_json_body_returns_none(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(200, json_body=None)
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._refresh(_bundle()) is None

    def test_missing_access_token_returns_none(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(200, {"refresh_token": "r2"})
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._refresh(_bundle()) is None


# ---------------------------------------------------------------------------
# _validate
# ---------------------------------------------------------------------------


class TestValidate:
    @pytest.mark.parametrize("status", [401, 403])
    def test_rejects_on_auth_failure(self, status):
        mgr = _manager()
        fake = Mock()
        fake.get.return_value = _response(status)
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._validate() is False

    @pytest.mark.parametrize("status", [429, 500, 503])
    def test_transient_status_does_not_demote_the_session(self, status):
        """A 429 must not cascade into a password login and a device challenge."""
        mgr = _manager()
        fake = Mock()
        fake.get.return_value = _response(status)
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._validate() is True

    def test_network_error_assumes_valid(self):
        mgr = _manager()
        fake = Mock()
        fake.get.side_effect = OSError("dns failure")
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._validate() is True

    def test_success(self):
        mgr = _manager()
        fake = Mock()
        fake.get.return_value = _response(200, {})
        with patch.object(session_mod, "SESSION", fake):
            assert mgr._validate() is True


# ---------------------------------------------------------------------------
# ensure_fresh
# ---------------------------------------------------------------------------


class TestEnsureFresh:
    def test_noop_when_token_is_young(self):
        mgr = _manager()
        mgr._bundle = _bundle()
        fake = Mock()
        with patch.object(session_mod, "SESSION", fake):
            assert mgr.ensure_fresh() is True
        fake.post.assert_not_called()

    def test_refreshes_when_due(self):
        now = time.time()
        mgr = _manager()
        mgr._bundle = _bundle(issued_at=now - 50_000, expires_at=now + 36_400)
        fake = Mock()
        fake.post.return_value = _response(200, {"access_token": "a2", "refresh_token": "r2"})
        with patch.object(session_mod, "SESSION", fake):
            assert mgr.ensure_fresh() is True
        assert mgr.bundle.access_token == "a2"

    def test_failed_refresh_keeps_a_still_valid_token(self):
        now = time.time()
        mgr = _manager()
        mgr._bundle = _bundle(issued_at=now - 50_000, expires_at=now + 36_400)
        fake = Mock()
        fake.post.return_value = _response(500)
        with patch.object(session_mod, "SESSION", fake), \
             patch.object(mgr, "authenticate") as auth:
            assert mgr.ensure_fresh() is True
        auth.assert_not_called()

    def test_expired_token_after_failed_refresh_reauthenticates(self):
        now = time.time()
        mgr = _manager()
        mgr._bundle = _bundle(issued_at=now - 90_000, expires_at=now - 10)
        fake = Mock()
        fake.post.return_value = _response(500)
        with patch.object(session_mod, "SESSION", fake), \
             patch.object(mgr, "authenticate", return_value=LoginOutcome.SUCCESS) as auth:
            assert mgr.ensure_fresh() is True
        auth.assert_called_once()

    def test_authenticates_when_no_bundle_yet(self):
        mgr = _manager()
        with patch.object(mgr, "authenticate", return_value=LoginOutcome.SUCCESS) as auth:
            assert mgr.ensure_fresh() is True
        auth.assert_called_once()


# ---------------------------------------------------------------------------
# password_login
# ---------------------------------------------------------------------------


class TestPasswordLogin:
    def test_success_persists_and_hydrates(self):
        backend = MemoryBackend()
        mgr = _manager(store=SessionStore([backend]))
        fake = Mock()
        fake.post.return_value = _response(
            200, {"access_token": "a", "refresh_token": "r", "expires_in": 86400}
        )
        with patch.object(session_mod, "SESSION", fake):
            assert mgr.password_login() == LoginOutcome.SUCCESS
        assert backend.data["session"]["access_token"] == "a"
        # Hydrated: the token is installed on robin_stocks' shared session so
        # every subsequent rh.* call is authenticated.
        assert real_session.headers["Authorization"] == "Bearer a"

    def test_payload_uses_pinned_device_token(self):
        mgr = _manager(device_token="pinned-device")
        fake = Mock()
        fake.post.return_value = _response(200, {"access_token": "a", "refresh_token": "r"})
        with patch.object(session_mod, "SESSION", fake):
            mgr.password_login()
        payload = fake.post.call_args.kwargs["data"]
        assert payload["device_token"] == "pinned-device"
        assert payload["grant_type"] == "password"
        assert payload["client_id"] == CLIENT_ID

    def test_totp_code_included(self):
        mgr = _manager(totp_secret="JBSWY3DPEHPK3PXP")
        fake = Mock()
        fake.post.return_value = _response(200, {"access_token": "a", "refresh_token": "r"})
        totp = Mock()
        totp.now.return_value = "123456"
        with patch.object(session_mod, "SESSION", fake), \
             patch.dict("sys.modules", {"pyotp": Mock(TOTP=Mock(return_value=totp))}):
            mgr.password_login()
        assert fake.post.call_args.kwargs["data"]["mfa_code"] == "123456"

    def test_totp_generation_failure_returns_unknown(self):
        mgr = _manager(totp_secret="bad-secret")
        with patch.object(mgr, "_totp_code", return_value=None):
            assert mgr.password_login() == LoginOutcome.UNKNOWN

    def test_verification_workflow_is_handled_then_retried(self):
        mgr = _manager()
        fake = Mock()
        fake.post.side_effect = [
            _response(200, {"verification_workflow": {"id": "wf-1"}}),
            _response(200, {"access_token": "a", "refresh_token": "r"}),
        ]
        with patch.object(session_mod, "SESSION", fake), \
             patch.object(_rs_auth, "_validate_sherrif_id") as validate:
            assert mgr.password_login() == LoginOutcome.SUCCESS
        validate.assert_called_once_with("device-1", "wf-1")

    def test_challenge_timeout_classifies_as_device_challenge(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(200, {"verification_workflow": {"id": "wf-1"}})
        with patch.object(session_mod, "SESSION", fake), \
             patch.object(
                 _rs_auth,
                 "_validate_sherrif_id",
                 side_effect=TimeoutError("device approval not validated within 300s"),
             ):
            assert mgr.password_login() == LoginOutcome.DEVICE_CHALLENGE

    def test_mfa_required_without_secret_is_a_device_challenge(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(200, {"mfa_required": True, "mfa_type": "sms"})
        with patch.object(session_mod, "SESSION", fake):
            assert mgr.password_login() == LoginOutcome.DEVICE_CHALLENGE

    def test_bad_credentials_from_detail(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(
            400, {"detail": "Unable to log in with provided credentials."}
        )
        with patch.object(session_mod, "SESSION", fake):
            assert mgr.password_login() == LoginOutcome.BAD_CREDENTIALS

    def test_429_classifies_rate_limited(self):
        mgr = _manager()
        fake = Mock()
        fake.post.return_value = _response(429)
        with patch.object(session_mod, "SESSION", fake):
            assert mgr.password_login() == LoginOutcome.RATE_LIMITED

    def test_network_error_classifies_transient(self):
        mgr = _manager()
        fake = Mock()
        fake.post.side_effect = OSError("Connection refused")
        with patch.object(session_mod, "SESSION", fake):
            assert mgr.password_login() == LoginOutcome.TRANSIENT

    def test_interactive_installs_and_clears_code_provider(self):
        from robinhood_sync import auth_patch

        mgr = _manager(interactive=True)
        fake = Mock()
        fake.post.return_value = _response(200, {"access_token": "a", "refresh_token": "r"})
        with patch.object(session_mod, "SESSION", fake):
            mgr.password_login()
        assert auth_patch._code_provider is None  # cleared afterwards


# ---------------------------------------------------------------------------
# Legacy pickle migration
# ---------------------------------------------------------------------------


class TestLegacyPickleImport:
    def _write_pickle(self, path, payload):
        import pickle

        with open(path, "wb") as f:
            pickle.dump(payload, f)

    def test_imports_tokens_and_device_identity(self, tmp_path):
        path = tmp_path / "robinhood.pickle"
        self._write_pickle(
            path,
            {
                "access_token": "a",
                "refresh_token": "r",
                "token_type": "Bearer",
                "device_token": "legacy-device",
            },
        )
        backend = MemoryBackend()
        mgr = _manager(store=SessionStore([backend]), device_token=None)
        assert mgr.import_legacy_pickle(str(path)) is True
        assert backend.data["session"]["refresh_token"] == "r"
        assert mgr.device_token == "legacy-device"

    def test_imported_bundle_is_due_for_refresh(self, tmp_path):
        path = tmp_path / "robinhood.pickle"
        self._write_pickle(path, {"access_token": "a", "refresh_token": "r"})
        mgr = _manager()
        mgr.import_legacy_pickle(str(path))
        assert mgr.bundle.needs_refresh() is True

    def test_missing_file(self, tmp_path):
        assert _manager().import_legacy_pickle(str(tmp_path / "nope.pickle")) is False

    def test_corrupt_pickle(self, tmp_path):
        path = tmp_path / "robinhood.pickle"
        path.write_bytes(b"not a pickle")
        assert _manager().import_legacy_pickle(str(path)) is False

    def test_pickle_without_access_token(self, tmp_path):
        path = tmp_path / "robinhood.pickle"
        self._write_pickle(path, {"device_token": "d"})
        assert _manager().import_legacy_pickle(str(path)) is False


# ---------------------------------------------------------------------------
# from_settings wiring
# ---------------------------------------------------------------------------


class TestFromSettings:
    def _settings(self, **overrides):
        s = Mock()
        s.robinhood_username = "u"
        s.robinhood_password = "p"
        s.robinhood_totp_secret = None
        s.robinhood_device_token = "dev"
        s.robinhood_session_file = "/tmp/session.json"
        s.redis_session_key = "robinhood:session"
        s.redis_host = "localhost"
        s.redis_port = 6379
        s.redis_db = 0
        s.redis_password = None
        s.session_redis_enabled = True
        s.session_expires_in = 86400
        s.session_refresh_ratio = 0.5
        s.session_refresh_margin_sec = 900
        for k, v in overrides.items():
            setattr(s, k, v)
        return s

    def test_builds_redis_and_file_backends(self):
        mgr = SessionManager.from_settings(self._settings())
        names = [b.name for b in mgr.store.backends]
        assert names == ["redis", "file"]

    def test_redis_can_be_disabled_by_config(self):
        mgr = SessionManager.from_settings(self._settings(session_redis_enabled=False))
        assert [b.name for b in mgr.store.backends] == ["file"]

    def test_redis_can_be_disabled_by_argument(self):
        mgr = SessionManager.from_settings(self._settings(), redis_enabled=False)
        assert [b.name for b in mgr.store.backends] == ["file"]

    def test_carries_policy_settings(self):
        mgr = SessionManager.from_settings(
            self._settings(session_refresh_ratio=0.25, session_refresh_margin_sec=60)
        )
        assert mgr.refresh_ratio == 0.25
        assert mgr.refresh_margin_sec == 60


# ---------------------------------------------------------------------------
# classify_login_failure — markers added with the new login path
# ---------------------------------------------------------------------------


class TestClassifierAdditions:
    def test_robinhood_credential_detail(self):
        assert classify_login_failure(
            "Unable to log in with provided credentials.", None
        ) == LoginOutcome.BAD_CREDENTIALS

    def test_invalid_grant_detail(self):
        assert classify_login_failure("invalid_grant", None) == LoginOutcome.BAD_CREDENTIALS

    def test_429_in_exception_text(self):
        assert classify_login_failure(
            "", RuntimeError("HTTP 429 Too Many Requests")
        ) == LoginOutcome.RATE_LIMITED

    def test_headless_challenge_exception(self):
        exc = RuntimeError(
            "auth_patch: Robinhood demanded sms verification — requires "
            "interactive input the Pi cannot provide."
        )
        assert classify_login_failure("", exc) == LoginOutcome.DEVICE_CHALLENGE
