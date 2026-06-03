"""
Minimal Telegram notifier for operational alerts.

Used by the login retry loop to alert a human when the service halts
on an unrecoverable auth failure (device-approval challenge or bad
credentials). Designed to be a graceful no-op if Telegram creds are
not configured, so unit tests and local runs don't blow up.

This is now a thin wrapper over the shared
``trading_commons.telegram.TelegramClient`` (httpx-based, with retry and a
no-op-without-creds fallback). Behavior is preserved: messages are sent as
plain text (``parse_mode=""``) to match the previous urllib-based notifier,
``send()`` returns ``True``/``False`` and never raises, and a notifier with
missing creds is a logged no-op.
"""

import logging
from typing import Optional

from trading_commons.telegram import TelegramClient

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Post text messages to a Telegram chat via the shared TelegramClient."""

    def __init__(
        self,
        bot_token: Optional[str],
        chat_id: Optional[str],
        timeout_seconds: float = 10.0,
    ):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout = timeout_seconds
        self._client = TelegramClient(
            bot_token=bot_token,
            chat_id=chat_id,
            timeout=timeout_seconds,
        )

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    def send(self, text: str) -> bool:
        """
        POST a message to the configured chat. Returns True on success,
        False on any failure (creds missing, network error, non-200).
        Never raises — the notifier is a best-effort side channel.

        Messages are sent as plain text (no parse mode) to preserve the
        original notifier's behavior.
        """
        if not self.enabled:
            logger.info("Telegram notifier disabled (no creds); skipping alert")
            return False
        # parse_mode="" => Telegram treats the message as plain text, matching
        # the prior urllib-based notifier exactly.
        return self._client.send_message(text, parse_mode="")
