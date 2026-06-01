"""Additional TradeEventProducer tests: connect, reconnect, send-retry,
and the error/exception branches of each publish method."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from kafka.errors import KafkaError

from robinhood_sync.robinhood_client import Trade, Position, AccountBalance
from robinhood_sync.kafka_producer import TradeEventProducer


def _producer_with_mock():
    p = TradeEventProducer(
        brokers=["localhost:9092"], topic="trading.orders",
        positions_topic="trading.positions", watchlist_topic="trading.watchlist",
    )
    mock_kafka = MagicMock()
    future = MagicMock()
    future.get.return_value = None
    mock_kafka.send.return_value = future
    p._producer = mock_kafka
    return p


def _trade():
    return Trade(
        order_id="t-1", symbol="AAPL", side="buy", quantity=Decimal("10"),
        average_price=Decimal("150"), total_notional=Decimal("1500"),
        fees=Decimal("0"), state="filled",
        executed_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
        created_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
        instrument_url="url",
    )


def _balance():
    return AccountBalance(
        buying_power=Decimal("500"), cash=Decimal("300"),
        total_equity=Decimal("1200"), updated_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------

class TestConnect:
    def test_connect_success(self):
        p = TradeEventProducer(brokers=["b:9092"], topic="t")
        with patch("robinhood_sync.kafka_producer.KafkaProducer") as KP:
            assert p.connect() is True
            KP.assert_called_once()
        assert p._producer is not None

    def test_connect_failure_returns_false(self):
        p = TradeEventProducer(brokers=["b:9092"], topic="t")
        with patch(
            "robinhood_sync.kafka_producer.KafkaProducer",
            side_effect=KafkaError("no brokers"),
        ):
            assert p.connect() is False


# ---------------------------------------------------------------------------
# close / reconnect
# ---------------------------------------------------------------------------

class TestCloseReconnect:
    def test_close_error_is_caught(self):
        p = _producer_with_mock()
        p._producer.flush.side_effect = RuntimeError("flush fail")
        p.close()  # no exception

    def test_reconnect_closes_old_and_reconnects(self):
        p = _producer_with_mock()
        old = p._producer
        with patch.object(p, "connect", return_value=True) as conn:
            assert p._reconnect_producer() is True
        old.close.assert_called_once()
        conn.assert_called_once()

    def test_reconnect_swallows_old_close_error(self):
        p = _producer_with_mock()
        p._producer.close.side_effect = RuntimeError("already closed")
        with patch.object(p, "connect", return_value=True):
            assert p._reconnect_producer() is True


# ---------------------------------------------------------------------------
# _send_with_retry
# ---------------------------------------------------------------------------

class TestSendWithRetry:
    def test_send_success(self):
        p = _producer_with_mock()
        p._send_with_retry("topic", "key", {"a": 1})
        p._producer.send.assert_called_once()

    def test_send_failure_reconnect_then_retry(self):
        p = _producer_with_mock()
        bad_future = MagicMock()
        bad_future.get.side_effect = KafkaError("send fail")
        good_future = MagicMock()
        good_future.get.return_value = None
        p._producer.send.side_effect = [bad_future, good_future]
        with patch.object(p, "_reconnect_producer", return_value=True):
            p._send_with_retry("topic", "key", {"a": 1})
        assert p._producer.send.call_count == 2

    def test_send_failure_reconnect_fails_raises(self):
        p = _producer_with_mock()
        bad_future = MagicMock()
        bad_future.get.side_effect = KafkaError("send fail")
        p._producer.send.return_value = bad_future
        with patch.object(p, "_reconnect_producer", return_value=False):
            with pytest.raises(KafkaError):
                p._send_with_retry("topic", "key", {"a": 1})


# ---------------------------------------------------------------------------
# publish error branches
# ---------------------------------------------------------------------------

class TestPublishErrorBranches:
    def test_publish_trade_kafka_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=KafkaError("x")):
            assert p.publish_trade(_trade()) is False

    def test_publish_trade_generic_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=RuntimeError("x")):
            assert p.publish_trade(_trade()) is False

    def test_publish_positions_kafka_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=KafkaError("x")):
            assert p.publish_positions([], _balance()) is False

    def test_publish_positions_generic_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=RuntimeError("x")):
            assert p.publish_positions([], _balance()) is False

    def test_publish_watchlist_update_kafka_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=KafkaError("x")):
            assert p.publish_watchlist_update(["A"], [], ["A"]) is False

    def test_publish_watchlist_update_generic_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=RuntimeError("x")):
            assert p.publish_watchlist_update(["A"], [], ["A"]) is False

    def test_publish_watchlist_update_not_connected_raises(self):
        p = TradeEventProducer(brokers=["b"], topic="t")
        with pytest.raises(RuntimeError):
            p.publish_watchlist_update([], [], [])

    def test_publish_symbol_added_kafka_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=KafkaError("x")):
            assert p.publish_symbol_added("AAPL") is False

    def test_publish_symbol_added_generic_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=RuntimeError("x")):
            assert p.publish_symbol_added("AAPL") is False

    def test_publish_symbol_removed_kafka_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=KafkaError("x")):
            assert p.publish_symbol_removed("AAPL") is False

    def test_publish_symbol_removed_generic_error(self):
        p = _producer_with_mock()
        with patch.object(p, "_send_with_retry", side_effect=RuntimeError("x")):
            assert p.publish_symbol_removed("AAPL") is False
