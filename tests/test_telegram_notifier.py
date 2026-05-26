"""Tests for the Telegram notifier — disabled mode + best-effort send."""

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
    with patch("robinhood_sync.telegram_notifier.urllib.request.urlopen",
               side_effect=Exception("boom")):
        assert n.send("hi") is False  # no raise, just False


def test_send_success():
    n = TelegramNotifier(bot_token="x", chat_id="y")
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b'{"ok": true}'
    mock_resp.__enter__.return_value = mock_resp
    mock_resp.__exit__.return_value = False
    with patch("robinhood_sync.telegram_notifier.urllib.request.urlopen",
               return_value=mock_resp):
        assert n.send("hi") is True


def test_send_non_ok_response():
    n = TelegramNotifier(bot_token="x", chat_id="y")
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b'{"ok": false, "description": "bad chat_id"}'
    mock_resp.__enter__.return_value = mock_resp
    mock_resp.__exit__.return_value = False
    with patch("robinhood_sync.telegram_notifier.urllib.request.urlopen",
               return_value=mock_resp):
        assert n.send("hi") is False
