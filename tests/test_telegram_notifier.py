"""Tests for the Telegram notifier — disabled mode + best-effort send.

The notifier now wraps trading_commons.telegram.TelegramClient; the underlying
httpx client is mocked so no network IO occurs.
"""

from unittest.mock import patch, MagicMock

from robinhood_sync.telegram_notifier import TelegramNotifier


def test_disabled_when_no_creds():
    n = TelegramNotifier(bot_token=None, chat_id=None)
    assert n.enabled is False
    # send() returns False without raising and without network IO
    assert n.send("hi") is False


def test_disabled_when_partial_creds():
    assert TelegramNotifier(bot_token="x", chat_id=None).enabled is False
    assert TelegramNotifier(bot_token=None, chat_id="y").enabled is False


def test_send_swallows_network_errors():
    n = TelegramNotifier(bot_token="x", chat_id="y")
    assert n.enabled is True
    # The shared client never raises; it returns False on transport failure.
    with patch.object(n._client, "send_message", return_value=False) as send:
        assert n.send("hi") is False  # no raise, just False
    send.assert_called_once_with("hi", parse_mode="")


def test_send_success():
    n = TelegramNotifier(bot_token="x", chat_id="y")
    with patch.object(n._client, "send_message", return_value=True) as send:
        assert n.send("hi") is True
    # plain-text parse mode preserves prior behavior
    send.assert_called_once_with("hi", parse_mode="")


def test_send_non_ok_response():
    n = TelegramNotifier(bot_token="x", chat_id="y")
    with patch.object(n._client, "send_message", return_value=False):
        assert n.send("hi") is False


def test_send_disabled_does_not_call_client():
    n = TelegramNotifier(bot_token=None, chat_id=None)
    with patch.object(n._client, "send_message") as send:
        assert n.send("hi") is False
    send.assert_not_called()


def test_uses_httpx_client_transport():
    """Verify the wrapper actually delegates to the shared httpx-based client."""
    n = TelegramNotifier(bot_token="x", chat_id="y")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    with patch("trading_commons.telegram.httpx.Client") as Client:
        Client.return_value.__enter__.return_value.post.return_value = mock_resp
        assert n.send("hi") is True
