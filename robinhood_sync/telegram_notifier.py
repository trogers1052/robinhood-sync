"""
Minimal Telegram notifier for operational alerts.

Used by the login retry loop to alert a human when the service halts
on an unrecoverable auth failure (device-approval challenge or bad
credentials). Designed to be a graceful no-op if Telegram creds are
not configured, so unit tests and local runs don't blow up.
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Post text messages to a Telegram chat via the Bot API."""

    API_BASE = "https://api.telegram.org"

    def __init__(
        self,
        bot_token: Optional[str],
        chat_id: Optional[str],
        timeout_seconds: float = 10.0,
    ):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    def send(self, text: str) -> bool:
        """
        POST a message to the configured chat. Returns True on success,
        False on any failure (creds missing, network error, non-200).
        Never raises — the notifier is a best-effort side channel.
        """
        if not self.enabled:
            logger.info("Telegram notifier disabled (no creds); skipping alert")
            return False

        url = f"{self.API_BASE}/bot{self.bot_token}/sendMessage"
        payload = urllib.parse.urlencode(
            {"chat_id": self.chat_id, "text": text}
        ).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                if resp.status != 200:
                    logger.error(
                        f"Telegram returned status={resp.status} body={body[:200]}"
                    )
                    return False
                parsed = json.loads(body) if body else {}
                if not parsed.get("ok", False):
                    logger.error(f"Telegram API not-ok: {body[:200]}")
                    return False
                return True
        except urllib.error.URLError as e:
            logger.error(f"Telegram send failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending Telegram message: {e}")
            return False
