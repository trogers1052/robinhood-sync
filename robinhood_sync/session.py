"""
Owned Robinhood session lifecycle — refresh-token first, password login last.

Why this module exists
----------------------
robin_stocks 3.4.0 loses the login session in three compounding ways, and the
Pi has paid for all three (2026-06-01 → 2026-07-24 outage, then again
2026-08-01 with a 205-restart crash-loop against Robinhood's login endpoint):

  1. **The refresh token is stored but never used.** ``rh.login()`` writes
     ``refresh_token`` into its pickle, then on the next start only replays the
     *access* token. When Robinhood expires that access token there is no
     refresh path at all — it falls straight back to a full password login.
  2. **A full password login mints a brand-new random ``device_token``**
     (``generate_device_token()`` at the top of ``login()``). The pickle's
     device token is only reused if the pickle loads cleanly. So the moment the
     pickle is missing or unreadable we present as a *new device*, Robinhood
     fires its device-approval workflow, and a headless Pi cannot answer it.
  3. **The pickle is the only copy of the session**, it lives in a container
     volume, and ``login()`` rewrites it with ``open(path, 'wb')`` on every
     attempt — a login that returns a challenge response instead of tokens
     truncates the file on its way to a ``KeyError``.

This module takes ownership of all three:

  * the token bundle is persisted to **Redis with a local file mirror**, so a
    recreated container or a wiped volume does not cost the session;
  * the ``device_token`` is **pinned** and persisted independently of the
    tokens, so a lost session still presents as the same trusted device;
  * the **refresh grant is used proactively** (at half the access token's life)
    and reactively (on a 401), so the password login — the only path that can
    trigger a device challenge — is a last resort rather than a daily event.

Rotation note: Robinhood rotates the refresh token on every refresh. The new
bundle is persisted *before* it is validated, because losing a rotated refresh
token breaks the chain and forces an interactive re-prime.
"""

import io
import json
import logging
import os
import time
from contextlib import redirect_stdout
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

import redis
import robin_stocks.robinhood.authentication as _rs_auth
from robin_stocks.robinhood.globals import SESSION
from robin_stocks.robinhood.helper import set_login_state, update_session

# Installs the 429-aware _validate_sherrif_id before any login runs.
from . import auth_patch
from .config import Settings
from .redis_client import _RedisBase

logger = logging.getLogger(__name__)


# Robinhood OAuth constants — same client_id robin_stocks uses.
CLIENT_ID = "c82SH0WZOsabOXGP2sxqcj34FxkvfnWRZBKlBjFS"
TOKEN_URL = "https://api.robinhood.com/oauth2/token/"
# Cheapest authenticated endpoint that reliably 401s on a dead token.
VALIDATE_URL = "https://api.robinhood.com/accounts/"

REQUEST_TIMEOUT_SEC = 16.0

# The original generator, captured before anything pins it.
_ORIGINAL_GENERATE_DEVICE_TOKEN = _rs_auth.generate_device_token


class LoginOutcome(str, Enum):
    """
    Classified result of an attempted Robinhood authentication.

    The retry loop in main.py uses this to decide whether to back off,
    halt and alert, or treat the failure as transient.
    """

    SUCCESS = "success"
    # Network/DNS/5xx — safe to retry with backoff
    TRANSIENT = "transient"
    # HTTP 429 from the login or device-approval endpoints — wait hours, not minutes
    RATE_LIMITED = "rate_limited"
    # Robinhood demands a device approval push on the user's phone.
    # The Pi cannot complete this; halt and alert a human.
    DEVICE_CHALLENGE = "device_challenge"
    # Credentials rejected outright with no challenge issued.
    BAD_CREDENTIALS = "bad_credentials"
    # Anything else we couldn't classify confidently.
    UNKNOWN = "unknown"

    @property
    def is_halt(self) -> bool:
        """True if this outcome requires human intervention (no retry)."""
        return self in (LoginOutcome.DEVICE_CHALLENGE, LoginOutcome.BAD_CREDENTIALS)


def classify_login_failure(stdout: str, exception: Optional[BaseException]) -> LoginOutcome:
    """
    Inspect captured output and the raised exception (if any) to classify a
    failed authentication attempt.

    Both robin_stocks and :mod:`robinhood_sync.auth_patch` print challenge
    state to stdout rather than raising, so the caller captures stdout and
    feeds it here along with Robinhood's own ``detail`` string.
    """
    stdout_lower = stdout.lower()
    exc_str = (str(exception) or "").lower() if exception else ""
    exc_type = type(exception).__name__ if exception else ""

    # Rate-limited — most urgent classification, must dominate over device-challenge
    # because a 429 *is* a device-challenge polling response, but treating it as
    # "challenge required" leads to more retries which makes the 429 worse.
    if "429" in stdout or "too many requests" in stdout_lower:
        return LoginOutcome.RATE_LIMITED
    if "429" in exc_str or "too many requests" in exc_str:
        return LoginOutcome.RATE_LIMITED
    if "'nonetype' object is not subscriptable" in exc_str or \
       "'nonetype' object is not subscriptable" in stdout_lower:
        return LoginOutcome.RATE_LIMITED

    # Device-approval challenge was demanded (regardless of whether it timed out
    # or the user approved late).
    challenge_markers = (
        "verification required",
        "verification_workflow",
        "device approvals",
        "approve the login request",
        "starting verification process",
        "requires interactive input",
    )
    if any(m in stdout_lower for m in challenge_markers):
        return LoginOutcome.DEVICE_CHALLENGE
    if any(m in exc_str for m in challenge_markers):
        return LoginOutcome.DEVICE_CHALLENGE
    if exc_type in ("TimeoutError",) and "verification" in exc_str:
        return LoginOutcome.DEVICE_CHALLENGE
    if "challenge_status" in exc_str or "'status'" in exc_str:
        return LoginOutcome.DEVICE_CHALLENGE

    # Bad creds — Robinhood's own rejection detail, plus robin_stocks's phrasing.
    credential_markers = (
        "check credentials",
        "incorrect login",
        "unable to log in with provided credentials",
        "invalid_grant",
    )
    if any(m in stdout_lower for m in credential_markers):
        return LoginOutcome.BAD_CREDENTIALS

    # Network-ish exceptions or unrecognized failure — safe to retry transiently.
    transient_markers = ("connection", "timeout", "temporarily unavailable", "dns",
                         "name or service not known", "max retries exceeded")
    if any(m in exc_str for m in transient_markers):
        return LoginOutcome.TRANSIENT

    return LoginOutcome.UNKNOWN


# ---------------------------------------------------------------------------
# Device token pinning
# ---------------------------------------------------------------------------


def generate_device_token() -> str:
    """Generate a Robinhood-format device token (delegates to robin_stocks)."""
    return _ORIGINAL_GENERATE_DEVICE_TOKEN()


def pin_device_token(device_token: str) -> None:
    """
    Force every robin_stocks code path to present *device_token*.

    ``rh.login()`` calls ``generate_device_token()`` unconditionally at the top
    of the function; without this pin, any fallback into robin_stocks' own
    login would present a brand-new device identity and re-trigger Robinhood's
    approval workflow. We drive the login ourselves, but the pin closes the
    door on any path we don't own.
    """
    _rs_auth.generate_device_token = lambda: device_token


# ---------------------------------------------------------------------------
# The persisted session bundle
# ---------------------------------------------------------------------------


@dataclass
class SessionBundle:
    """An OAuth token set plus the device identity that minted it."""

    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    device_token: str = ""
    issued_at: float = 0.0
    expires_at: float = 0.0
    scope: str = "internal"
    username: str = ""

    @classmethod
    def from_token_response(
        cls,
        data: dict,
        device_token: str,
        username: str = "",
        now: Optional[float] = None,
        default_expires_in: int = 86400,
    ) -> "SessionBundle":
        """Build a bundle from Robinhood's ``/oauth2/token/`` response body."""
        now = time.time() if now is None else now
        try:
            expires_in = float(data.get("expires_in") or default_expires_in)
        except (TypeError, ValueError):
            expires_in = float(default_expires_in)
        return cls(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token") or "",
            token_type=data.get("token_type") or "Bearer",
            device_token=device_token,
            issued_at=now,
            expires_at=now + expires_in,
            scope=data.get("scope") or "internal",
            username=username,
        )

    @classmethod
    def from_dict(cls, data: dict) -> Optional["SessionBundle"]:
        """Rebuild from a persisted dict. Returns None if it isn't usable."""
        if not isinstance(data, dict) or not data.get("access_token"):
            return None
        try:
            return cls(
                access_token=str(data["access_token"]),
                refresh_token=str(data.get("refresh_token") or ""),
                token_type=str(data.get("token_type") or "Bearer"),
                device_token=str(data.get("device_token") or ""),
                issued_at=float(data.get("issued_at") or 0.0),
                expires_at=float(data.get("expires_at") or 0.0),
                scope=str(data.get("scope") or "internal"),
                username=str(data.get("username") or ""),
            )
        except (TypeError, ValueError) as e:
            logger.warning(f"Discarding malformed session bundle: {e}")
            return None

    def to_dict(self) -> dict:
        return asdict(self)

    def seconds_remaining(self, now: Optional[float] = None) -> float:
        now = time.time() if now is None else now
        return self.expires_at - now

    def is_expired(self, now: Optional[float] = None, margin_sec: float = 0.0) -> bool:
        return self.seconds_remaining(now) <= margin_sec

    def needs_refresh(
        self,
        now: Optional[float] = None,
        ratio: float = 0.5,
        margin_sec: float = 900.0,
    ) -> bool:
        """
        True once the access token is past *ratio* of its life, or has less
        than *margin_sec* left. Refreshing early is free; refreshing late costs
        a password login and a possible device challenge.
        """
        now = time.time() if now is None else now
        if self.seconds_remaining(now) <= margin_sec:
            return True
        lifetime = self.expires_at - self.issued_at
        if lifetime <= 0:
            # Unknown lifetime (e.g. imported legacy pickle) — refresh now.
            return True
        return (now - self.issued_at) >= (lifetime * ratio)

    def authorization_header(self) -> str:
        return f"{self.token_type} {self.access_token}"

    def redacted(self) -> str:
        """Loggable one-liner. Never logs token material."""
        remaining = self.seconds_remaining()
        return (
            f"SessionBundle(device={self.device_token[:8]}…, "
            f"expires_in={remaining / 3600:.1f}h, "
            f"refreshable={'yes' if self.refresh_token else 'NO'})"
        )


@dataclass
class SessionDocument:
    """
    What actually gets persisted: the device identity plus the current bundle.

    The device token is stored *outside* the bundle as well, so that an
    expired-and-unrefreshable session still leaves the device identity behind
    for the next password login to reuse.
    """

    device_token: Optional[str] = None
    bundle: Optional[SessionBundle] = None
    updated_at: float = field(default_factory=time.time)

    @classmethod
    def from_dict(cls, data: dict) -> "SessionDocument":
        if not isinstance(data, dict):
            return cls()
        bundle = SessionBundle.from_dict(data.get("session") or {})
        device = data.get("device_token") or (bundle.device_token if bundle else None)
        try:
            updated_at = float(data.get("updated_at") or 0.0)
        except (TypeError, ValueError):
            updated_at = 0.0
        return cls(device_token=device or None, bundle=bundle, updated_at=updated_at)

    def to_dict(self) -> dict:
        return {
            "device_token": self.device_token,
            "session": self.bundle.to_dict() if self.bundle else None,
            "updated_at": self.updated_at,
        }

    @property
    def freshness(self) -> float:
        """Sort key for picking the newest copy across backends."""
        if self.bundle and self.bundle.issued_at:
            return max(self.bundle.issued_at, self.updated_at)
        return self.updated_at


# ---------------------------------------------------------------------------
# Storage backends
# ---------------------------------------------------------------------------


class FileSessionBackend:
    """
    Local JSON mirror of the session document.

    Writes are atomic (temp file + ``os.replace``) and mode 0600, so a crash
    mid-write cannot leave a half-written document behind — the failure mode
    that made the pickle so fragile.
    """

    name = "file"

    def __init__(self, path: str):
        self.path = Path(os.path.expanduser(path))

    def load(self) -> Optional[dict]:
        try:
            if not self.path.is_file():
                return None
            with self.path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning(f"Session file unreadable ({self.path}): {e}")
            return None

    def save(self, data: dict) -> bool:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(data, f)
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.path)
            return True
        except OSError as e:
            logger.warning(f"Failed to write session file {self.path}: {e}")
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return False

    def clear(self) -> None:
        try:
            self.path.unlink(missing_ok=True)
        except OSError as e:
            logger.warning(f"Failed to remove session file {self.path}: {e}")


class RedisSessionBackend(_RedisBase):
    """
    Redis copy of the session document — the source of truth.

    Redis outlives container recreation and volume wipes, and it is reachable
    from the Mac, which is what lets ``prime_session`` push a freshly-minted
    session to the Pi without copying files around.
    """

    name = "redis"

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.key = settings.redis_session_key

    def connect(self) -> bool:
        """Connect to Redis. Never raises — the file mirror is the fallback."""
        try:
            self._client = self._create_client()
            self._client.ping()
            logger.info(
                f"Session store connected to Redis at "
                f"{self.settings.redis_host}:{self.settings.redis_port} (key={self.key})"
            )
            return True
        except redis.RedisError as e:
            logger.warning(f"Session store could not reach Redis: {e}")
            self._client = None
            return False

    def load(self) -> Optional[dict]:
        if self._client is None and not self.connect():
            return None
        try:
            raw = self._client.get(self.key)
            if not raw:
                return None
            return json.loads(raw)
        except (redis.RedisError, json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to read session from Redis: {e}")
            return None

    def save(self, data: dict) -> bool:
        if self._client is None and not self.connect():
            return False
        try:
            # No TTL: expiry lives inside the payload. A key that outlives its
            # tokens still carries the device identity, which is the part we
            # most need to keep.
            self._client.set(self.key, json.dumps(data))
            return True
        except (redis.RedisError, TypeError) as e:
            logger.warning(f"Failed to write session to Redis: {e}")
            return False

    def clear(self) -> None:
        if self._client is None and not self.connect():
            return
        try:
            self._client.delete(self.key)
        except redis.RedisError as e:
            logger.warning(f"Failed to clear session in Redis: {e}")


class SessionStore:
    """
    Reads from every backend and uses the newest document; writes to all.

    Newest-wins means a session primed from the Mac straight into Redis is
    picked up by the Pi even though the Pi's file mirror still holds an older
    copy, and a Redis flush is silently healed from the file mirror on the next
    save.
    """

    def __init__(self, backends: list):
        self.backends = [b for b in backends if b is not None]

    def load(self) -> SessionDocument:
        best: Optional[SessionDocument] = None
        for backend in self.backends:
            try:
                raw = backend.load()
            except Exception as e:  # noqa: BLE001 — a bad backend must not block auth
                logger.warning(f"Session backend {backend.name} failed to load: {e}")
                continue
            if not raw:
                continue
            doc = SessionDocument.from_dict(raw)
            if best is None or doc.freshness > best.freshness:
                best = doc
        if best is None:
            logger.info("No persisted Robinhood session found in any backend")
            return SessionDocument()
        logger.info(
            f"Loaded persisted Robinhood session "
            f"({best.bundle.redacted() if best.bundle else 'device token only'})"
        )
        return best

    def save(self, doc: SessionDocument) -> int:
        doc.updated_at = time.time()
        data = doc.to_dict()
        written = 0
        for backend in self.backends:
            try:
                if backend.save(data):
                    written += 1
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Session backend {backend.name} failed to save: {e}")
        if written == 0 and self.backends:
            logger.error(
                "Session could not be persisted to ANY backend — the next "
                "restart will need a fresh login"
            )
        return written

    def clear(self) -> None:
        for backend in self.backends:
            try:
                backend.clear()
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Session backend {backend.name} failed to clear: {e}")

    def close(self) -> None:
        for backend in self.backends:
            closer = getattr(backend, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception as e:  # noqa: BLE001 — shutdown must not fail
                    logger.debug(f"Closing session backend {backend.name} failed: {e}")


# ---------------------------------------------------------------------------
# The session manager
# ---------------------------------------------------------------------------


class SessionManager:
    """
    Owns authentication end to end: resume → refresh → (last resort) password.

    The ordering is the whole point. A password login is the only path that can
    trigger Robinhood's device-approval workflow, and a headless Pi cannot
    answer one — so it is reached only when the persisted session is gone or
    its refresh token has been rejected.
    """

    def __init__(
        self,
        username: str,
        password: str,
        totp_secret: Optional[str] = None,
        store: Optional[SessionStore] = None,
        device_token: Optional[str] = None,
        expires_in: int = 86400,
        refresh_ratio: float = 0.5,
        refresh_margin_sec: int = 900,
        interactive: bool = False,
        allow_password_login: bool = True,
    ):
        self.username = username
        self.password = password
        self.totp_secret = totp_secret
        self.store = store if store is not None else SessionStore([])
        self.configured_device_token = device_token or None
        self.expires_in = expires_in
        self.refresh_ratio = refresh_ratio
        self.refresh_margin_sec = refresh_margin_sec
        self.interactive = interactive
        self.allow_password_login = allow_password_login

        self._device_token: Optional[str] = None
        self._bundle: Optional[SessionBundle] = None
        self._last_outcome: LoginOutcome = LoginOutcome.UNKNOWN
        # Set by _refresh() so callers can distinguish "refresh failed" from
        # "refresh was rate-limited" — the latter must never escalate to a
        # password login.
        self._rate_limited = False

    # -- construction -------------------------------------------------------

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        interactive: bool = False,
        redis_enabled: Optional[bool] = None,
    ) -> "SessionManager":
        """Build a manager with the Redis + file-mirror store from config."""
        use_redis = settings.session_redis_enabled if redis_enabled is None else redis_enabled
        backends = []
        if use_redis:
            backends.append(RedisSessionBackend(settings))
        backends.append(FileSessionBackend(settings.robinhood_session_file))
        return cls(
            username=settings.robinhood_username,
            password=settings.robinhood_password,
            totp_secret=settings.robinhood_totp_secret,
            store=SessionStore(backends),
            device_token=settings.robinhood_device_token,
            expires_in=settings.session_expires_in,
            refresh_ratio=settings.session_refresh_ratio,
            refresh_margin_sec=settings.session_refresh_margin_sec,
            interactive=interactive,
        )

    # -- public API ---------------------------------------------------------

    @property
    def device_token(self) -> str:
        """The pinned device identity, resolved lazily and persisted once set."""
        if self._device_token is None:
            self._resolve_device_token(SessionDocument())
        return self._device_token or ""

    @property
    def bundle(self) -> Optional[SessionBundle]:
        return self._bundle

    @property
    def last_outcome(self) -> LoginOutcome:
        return self._last_outcome

    def authenticate(self) -> LoginOutcome:
        """
        Establish an authenticated robin_stocks session.

        Tries, in order: the persisted bundle (refreshing it if due), a forced
        refresh, and only then a password login.
        """
        doc = self.store.load()
        self._resolve_device_token(doc)

        if doc.bundle:
            self._bundle = doc.bundle
            outcome = self._resume(doc.bundle)
            if outcome == LoginOutcome.SUCCESS:
                return self._finish(LoginOutcome.SUCCESS)
            if outcome == LoginOutcome.RATE_LIMITED:
                # Do not escalate to a password login while rate-limited — that
                # is exactly how the 205-restart crash-loop happened.
                return self._finish(LoginOutcome.RATE_LIMITED)

        if not self.allow_password_login:
            logger.error(
                "No usable persisted session and password login is disabled — "
                "run `python -m robinhood_sync.prime_session` to mint one"
            )
            return self._finish(LoginOutcome.DEVICE_CHALLENGE)

        return self._finish(self.password_login())

    def ensure_fresh(self) -> bool:
        """
        Cheap pre-cycle check: refresh the access token if it is past half its
        life. A no-op the vast majority of the time.

        Returns True if the session is usable afterwards.
        """
        if self._bundle is None:
            return self.authenticate() == LoginOutcome.SUCCESS
        if not self._bundle.needs_refresh(
            ratio=self.refresh_ratio, margin_sec=self.refresh_margin_sec
        ):
            return True
        logger.info(
            f"Access token past {self.refresh_ratio:.0%} of its life — refreshing proactively"
        )
        refreshed = self._refresh(self._bundle)
        if refreshed:
            self._hydrate(refreshed)
            return True
        # Refresh failed but the current token may still be valid — only
        # escalate once it is actually expired.
        if not self._bundle.is_expired():
            logger.warning("Proactive refresh failed; current access token is still valid")
            return True
        return self.authenticate() == LoginOutcome.SUCCESS

    def password_login(self) -> LoginOutcome:
        """
        Full username/password login against ``/oauth2/token/``.

        Driven here rather than through ``rh.login()`` so that the pinned
        device token is used, the challenge flow goes through our 429-aware
        patch, and the resulting tokens (including ``expires_in``) land in our
        own store instead of a truncatable pickle.
        """
        logger.info(f"Password login to Robinhood as {self.username}...")

        mfa_code = self._totp_code()
        if mfa_code is None and self.totp_secret:
            return LoginOutcome.UNKNOWN

        payload = {
            "client_id": CLIENT_ID,
            "expires_in": self.expires_in,
            "grant_type": "password",
            "password": self.password,
            "scope": "internal",
            "username": self.username,
            "device_token": self.device_token,
            "try_passkeys": False,
            "token_request_path": "/login",
            "create_read_only_secondary_token": True,
        }
        if mfa_code:
            payload["mfa_code"] = mfa_code

        # auth_patch prints challenge state rather than raising; capture it so
        # the classifier can read it.
        captured = io.StringIO()
        exc: Optional[BaseException] = None
        data: Optional[dict] = None
        detail = ""

        if self.interactive:
            auth_patch.set_code_provider(_prompt_for_code)

        try:
            with redirect_stdout(captured):
                data = self._post_token(payload)
                if isinstance(data, dict) and "verification_workflow" in data:
                    print("Verification required, handling challenge...")
                    workflow_id = (data.get("verification_workflow") or {}).get("id")
                    _rs_auth._validate_sherrif_id(self.device_token, workflow_id)
                    # Re-issue the login now that the workflow is approved.
                    data = self._post_token(payload)
        except BaseException as e:  # noqa: BLE001 — classified below
            exc = e
        finally:
            if self.interactive:
                auth_patch.set_code_provider(None)

        stdout = captured.getvalue()
        for line in stdout.strip().splitlines():
            if line.strip():
                logger.info(f"robinhood-auth: {line}")

        if isinstance(data, dict):
            detail = str(data.get("detail") or "")
            if data.get("mfa_required") and "access_token" not in data:
                logger.error(
                    f"Robinhood requires MFA (type={data.get('mfa_type')}) and no "
                    f"usable TOTP secret is configured"
                )
                return LoginOutcome.DEVICE_CHALLENGE
            if data.get("access_token"):
                bundle = SessionBundle.from_token_response(
                    data,
                    device_token=self.device_token,
                    username=self.username,
                    default_expires_in=self.expires_in,
                )
                self._persist(bundle)
                self._hydrate(bundle)
                logger.info(f"Password login succeeded — {bundle.redacted()}")
                return LoginOutcome.SUCCESS

        outcome = classify_login_failure(f"{stdout}\n{detail}", exc)
        if exc:
            logger.error(f"Password login failed: outcome={outcome.value} exc={exc!r}")
        else:
            logger.error(f"Password login failed: outcome={outcome.value} detail={detail!r}")
        return outcome

    def import_legacy_pickle(self, pickle_path: str) -> bool:
        """
        Adopt a robin_stocks pickle written by an earlier deployment.

        Makes the cutover seamless where a valid pickle still exists: the
        tokens and — more importantly — the already-trusted device token are
        carried into the new store. Lifetime is unknown, so the bundle is
        marked due for immediate refresh.
        """
        import pickle  # local import: only needed on the migration path

        path = Path(os.path.expanduser(pickle_path))
        if not path.is_file():
            return False
        try:
            with path.open("rb") as f:
                data = pickle.load(f)
        except Exception as e:  # noqa: BLE001 — a corrupt pickle is not fatal
            logger.warning(f"Could not read legacy pickle {path}: {e}")
            return False

        if not isinstance(data, dict) or not data.get("access_token"):
            return False

        now = time.time()
        bundle = SessionBundle(
            access_token=str(data["access_token"]),
            refresh_token=str(data.get("refresh_token") or ""),
            token_type=str(data.get("token_type") or "Bearer"),
            device_token=str(data.get("device_token") or self.device_token),
            # Unknown issue time — a zero-length lifetime makes needs_refresh()
            # true immediately, which is the behavior we want.
            issued_at=now,
            expires_at=now,
            username=self.username,
        )
        if bundle.device_token:
            self._device_token = bundle.device_token
            pin_device_token(bundle.device_token)
        self._persist(bundle)
        logger.info(f"Imported legacy robin_stocks pickle from {path}")
        return True

    def logout(self) -> None:
        """Drop the in-process session. The persisted bundle is left intact."""
        update_session("Authorization", None)
        set_login_state(False)

    def close(self) -> None:
        self.store.close()

    # -- internals ----------------------------------------------------------

    def _finish(self, outcome: LoginOutcome) -> LoginOutcome:
        self._last_outcome = outcome
        return outcome

    def _resolve_device_token(self, doc: SessionDocument) -> None:
        """
        Settle on one device identity and pin it for the process.

        Precedence: explicit config → persisted → freshly generated. A
        generated token is persisted immediately, so it survives even if the
        login that follows never completes.
        """
        token = (
            self.configured_device_token
            or doc.device_token
            or (doc.bundle.device_token if doc.bundle else None)
        )
        newly_generated = False
        if not token:
            token = generate_device_token()
            newly_generated = True
            logger.info(
                "No device token configured or persisted — generated a new one. "
                "The first login with it will need a device approval; it is "
                "persisted so this happens exactly once."
            )
        self._device_token = token
        pin_device_token(token)
        if newly_generated or doc.device_token != token:
            self.store.save(SessionDocument(device_token=token, bundle=doc.bundle))

    def _resume(self, bundle: SessionBundle) -> LoginOutcome:
        """Try to make *bundle* usable, refreshing if due or if it is rejected."""
        if bundle.needs_refresh(ratio=self.refresh_ratio, margin_sec=self.refresh_margin_sec):
            refreshed = self._refresh(bundle)
            if refreshed:
                bundle = refreshed
            elif self._rate_limited:
                return LoginOutcome.RATE_LIMITED

        if not bundle.is_expired():
            self._hydrate(bundle)
            if self._validate():
                logger.info(f"Resumed persisted Robinhood session — {bundle.redacted()}")
                return LoginOutcome.SUCCESS
            logger.info("Persisted access token was rejected — forcing a refresh")

        refreshed = self._refresh(bundle)
        if refreshed:
            self._hydrate(refreshed)
            if self._validate():
                logger.info(f"Refreshed Robinhood session — {refreshed.redacted()}")
                return LoginOutcome.SUCCESS
            logger.warning("Refreshed token was itself rejected")
        if self._rate_limited:
            return LoginOutcome.RATE_LIMITED
        return LoginOutcome.UNKNOWN

    def _refresh(self, bundle: SessionBundle) -> Optional[SessionBundle]:
        """
        Exchange the refresh token for a new access token.

        Robinhood rotates the refresh token here, so the new bundle is
        persisted before anything else can fail.
        """
        self._rate_limited = False
        if not bundle.refresh_token:
            logger.warning("Persisted session has no refresh token — cannot refresh")
            return None

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": bundle.refresh_token,
            "client_id": CLIENT_ID,
            "scope": bundle.scope or "internal",
            "expires_in": self.expires_in,
            "device_token": self.device_token,
        }
        try:
            res = SESSION.post(TOKEN_URL, data=payload, timeout=REQUEST_TIMEOUT_SEC)
        except Exception as e:  # noqa: BLE001 — network blip, caller decides
            logger.warning(f"Refresh request failed: {e}")
            return None

        if res.status_code == 429:
            self._rate_limited = True
            logger.error("Refresh rate-limited (429) — backing off, NOT falling back to login")
            return None
        if res.status_code != 200:
            body = _safe_body(res)
            logger.warning(
                f"Refresh rejected (HTTP {res.status_code}): {body[:200]} — "
                f"a password login will be required"
            )
            return None

        try:
            data = res.json()
        except ValueError:
            logger.warning("Refresh returned a non-JSON body")
            return None
        if not isinstance(data, dict) or not data.get("access_token"):
            logger.warning("Refresh returned no access_token")
            return None

        refreshed = SessionBundle.from_token_response(
            data,
            device_token=self.device_token,
            username=bundle.username or self.username,
            default_expires_in=self.expires_in,
        )
        if not refreshed.refresh_token:
            # Some responses omit it; keep the chain alive with the old one.
            refreshed.refresh_token = bundle.refresh_token
        self._persist(refreshed)
        logger.info(f"Refreshed Robinhood access token — {refreshed.redacted()}")
        return refreshed

    def _hydrate(self, bundle: SessionBundle) -> None:
        """Install *bundle* into robin_stocks' global session."""
        update_session("Authorization", bundle.authorization_header())
        set_login_state(True)
        self._bundle = bundle

    def _validate(self) -> bool:
        """
        Confirm the installed token is accepted.

        Only an explicit 401/403 counts as invalid. A 429 or a 5xx must NOT
        demote us to a password login — that turns a transient Robinhood
        hiccup into a device challenge the Pi cannot answer.
        """
        try:
            res = SESSION.get(VALIDATE_URL, timeout=REQUEST_TIMEOUT_SEC)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Session validation request failed: {e} — assuming still valid")
            return True
        if res.status_code in (401, 403):
            return False
        if res.status_code >= 400:
            logger.warning(
                f"Session validation returned HTTP {res.status_code} — treating the "
                f"token as valid rather than forcing a password login"
            )
        return True

    def _persist(self, bundle: SessionBundle) -> None:
        self._bundle = bundle
        self.store.save(
            SessionDocument(device_token=bundle.device_token or self.device_token, bundle=bundle)
        )

    def _post_token(self, payload: dict) -> Optional[dict]:
        """POST the login payload, returning the parsed body (or None)."""
        res = SESSION.post(TOKEN_URL, data=payload, timeout=REQUEST_TIMEOUT_SEC)
        if res.status_code == 429:
            print("429 Too Many Requests from Robinhood login endpoint")
            return None
        try:
            return res.json()
        except ValueError:
            print(f"Login returned HTTP {res.status_code} with a non-JSON body")
            return None

    def _totp_code(self) -> Optional[str]:
        """Generate a TOTP code, or None if none is configured/derivable."""
        if not self.totp_secret:
            return ""
        try:
            import pyotp

            code = pyotp.TOTP(self.totp_secret).now()
            logger.info("Generated TOTP code for 2FA")
            return code
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error generating TOTP code: {e}")
            return None


def _safe_body(res) -> str:
    try:
        return res.text or ""
    except Exception:  # noqa: BLE001
        return ""


def _prompt_for_code(challenge_type: str) -> str:
    """Interactive code prompt used by prime_session (never on the Pi)."""
    return input(f"Enter the {challenge_type} verification code Robinhood sent: ").strip()


def build_challenge_responder() -> Callable[[str], str]:
    """Exposed for tests/callers that want the default interactive provider."""
    return _prompt_for_code
