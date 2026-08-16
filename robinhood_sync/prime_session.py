"""
Interactive session primer — run this from the Mac, never on the Pi.

The Pi cannot answer Robinhood's device-approval challenge: there is no phone
prompt to tap and no terminal to type an SMS code into. This tool does that one
interactive login from a machine where a human *is* present, then writes the
resulting session straight into the store the Pi reads (Redis, plus the local
file mirror). The Pi then resumes and refreshes that session indefinitely.

Typical use::

    cd ~/Projects/trading-platform/robinhood-sync
    REDIS_HOST=100.75.98.35 REDIS_PASSWORD=... python -m robinhood_sync.prime_session

The device token matters as much as the tokens. Robinhood challenges *new*
devices, so priming reuses the pinned ``ROBINHOOD_DEVICE_TOKEN`` when one is
configured, and prints a freshly generated one for you to record as a secret
when it isn't. Same device token → no repeat challenges.
"""

import argparse
import getpass
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from .config import Settings, get_settings
from .session import LoginOutcome, SessionManager

logger = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _load_settings(args: argparse.Namespace) -> Optional[Settings]:
    """
    Load Settings, prompting for whichever credential is missing.

    Priming is often run on a machine that has no .env, so a missing
    credential is a prompt rather than a hard failure.
    """
    try:
        return get_settings()
    except Exception as e:  # noqa: BLE001 — pydantic ValidationError and friends
        logger.info(f"Settings incomplete ({e.__class__.__name__}); prompting for credentials")

    username = args.username or input("Robinhood username (email): ").strip()
    password = getpass.getpass("Robinhood password: ")
    if not username or not password:
        logger.error("Username and password are both required")
        return None
    try:
        return Settings(robinhood_username=username, robinhood_password=password)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Could not build settings: {e}")
        return None


def prime(args: argparse.Namespace) -> int:
    """Run the interactive login and persist the resulting session."""
    settings = _load_settings(args)
    if settings is None:
        return 1

    if args.device_token:
        settings.robinhood_device_token = args.device_token

    manager = SessionManager.from_settings(
        settings,
        interactive=True,
        redis_enabled=not args.no_redis,
    )

    print("=" * 68)
    print("Robinhood session primer")
    print("=" * 68)
    print(f"Account:      {settings.robinhood_username}")
    print(f"Device token: {manager.device_token}")
    print(f"Redis:        {'disabled' if args.no_redis else f'{settings.redis_host}:{settings.redis_port}'}")
    print(f"File mirror:  {settings.robinhood_session_file}")
    print("=" * 68)

    if args.reuse and not args.force:
        # Cheapest possible path: if the store already holds a refreshable
        # session, no login (and so no challenge) is needed at all.
        outcome = manager.authenticate()
        if outcome == LoginOutcome.SUCCESS:
            _report(manager, refreshed_only=True)
            return 0
        logger.info(f"Could not resume the persisted session ({outcome.value}) — logging in")

    if args.import_pickle:
        if manager.import_legacy_pickle(args.import_pickle):
            print(f"Imported legacy pickle from {args.import_pickle}")
            if manager.ensure_fresh():
                _report(manager, refreshed_only=True)
                return 0
            logger.warning("Imported pickle could not be refreshed — falling back to login")
        else:
            logger.warning(f"No usable pickle at {args.import_pickle}")

    print(
        "\nLogging in. If Robinhood challenges this device:\n"
        "  • a push prompt → approve it in the Robinhood app;\n"
        "  • an SMS/email code → type it here when asked.\n"
    )

    outcome = manager.password_login()
    if outcome != LoginOutcome.SUCCESS:
        print(f"\n❌ Priming failed: {outcome.value}")
        if outcome == LoginOutcome.RATE_LIMITED:
            print(
                "   Robinhood is rate-limiting this account. Wait a few hours "
                "before retrying — repeated attempts extend the block."
            )
        elif outcome == LoginOutcome.BAD_CREDENTIALS:
            print("   Check the username/password (and the Pi's Docker secrets).")
        return 1

    _report(manager)
    return 0


def _report(manager: SessionManager, refreshed_only: bool = False) -> None:
    """Print what was written and what to do next."""
    bundle = manager.bundle
    expires = (
        datetime.fromtimestamp(bundle.expires_at, tz=timezone.utc).astimezone()
        if bundle
        else None
    )
    print("\n" + "=" * 68)
    print("✅ Session " + ("refreshed" if refreshed_only else "primed") + " and persisted")
    print("=" * 68)
    if expires:
        print(f"Access token expires: {expires:%Y-%m-%d %H:%M %Z}")
    print(f"Refresh token:        {'present' if bundle and bundle.refresh_token else 'MISSING'}")
    print(f"Device token:         {manager.device_token}")
    print(
        "\nNext steps:\n"
        "  1. Pin the device token so it survives a wiped store:\n"
        f"       printf '%s' '{manager.device_token}' | ssh <pi> "
        "'sudo tee /mnt/data/secrets/robinhood_device_token >/dev/null'\n"
        "     (and add `robinhood_device_token` to the service's compose secrets)\n"
        "  2. Start the service:\n"
        "       ssh <pi> 'cd ~/Projects && docker compose up -d robinhood-sync'\n"
        "  3. Confirm it resumed rather than logged in:\n"
        "       ssh <pi> 'docker logs --tail 30 projects-robinhood-sync-1'\n"
        "     Look for \"Resumed persisted Robinhood session\".\n"
    )


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m robinhood_sync.prime_session",
        description=(
            "Mint a Robinhood session interactively and persist it where the "
            "headless sync service can resume it."
        ),
    )
    parser.add_argument("--username", help="Robinhood username (prompted if omitted)")
    parser.add_argument(
        "--device-token",
        help="Pin a specific device token instead of the configured/persisted one",
    )
    parser.add_argument(
        "--no-redis",
        action="store_true",
        help="Write only the local file mirror (skip Redis)",
    )
    parser.add_argument(
        "--reuse",
        action="store_true",
        help="Try to resume/refresh the persisted session before logging in",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Always do a full password login, even if a session could be resumed",
    )
    parser.add_argument(
        "--import-pickle",
        metavar="PATH",
        help="Adopt an existing robin_stocks pickle instead of logging in",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)
    try:
        return prime(args)
    except KeyboardInterrupt:
        print("\nAborted.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
