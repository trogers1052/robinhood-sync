"""Tests for TradeEventProducer — event structure, error handling, retry logic."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch, PropertyMock

import pytest

from robinhood_sync.robinhood_client import (
    Trade,
    Position,
    AccountBalance,
    WatchlistStock,
)
from robinhood_sync.kafka_producer import TradeEventProducer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_trade(**overrides):
    defaults = dict(
        order_id="t-001",
        symbol="AAPL",
        side="buy",
        quantity=Decimal("10"),
        average_price=Decimal("150.00"),
        total_notional=Decimal("1500.00"),
        fees=Decimal("0"),
        state="filled",
        executed_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
        created_at=datetime(2026, 2, 20, 14, 0, tzinfo=timezone.utc),
        instrument_url="https://api.robinhood.com/instruments/1/",
    )
    defaults.update(overrides)
    return Trade(**defaults)


def _make_position(**overrides):
    defaults = dict(
        symbol="AAPL",
        quantity=Decimal("10"),
        average_buy_price=Decimal("150.00"),
        equity=Decimal("1600.00"),
        percent_change=Decimal("6.67"),
        equity_change=Decimal("100.00"),
        updated_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return Position(**defaults)


def _make_balance(**overrides):
    defaults = dict(
        buying_power=Decimal("500.00"),
        cash=Decimal("300.00"),
        total_equity=Decimal("1200.00"),
        updated_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return AccountBalance(**defaults)


def _make_watchlist_stock(**overrides):
    defaults = dict(
        symbol="AAPL",
        name="Apple Inc.",
        instrument_url="https://api.robinhood.com/instruments/1/",
        added_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return WatchlistStock(**defaults)


@pytest.fixture
def producer():
    """Create a producer with a mock KafkaProducer."""
    p = TradeEventProducer(
        brokers=["localhost:9092"],
        topic="trading.orders",
        positions_topic="trading.positions",
        watchlist_topic="trading.watchlist",
    )
    mock_kafka = MagicMock()
    future = MagicMock()
    future.get.return_value = None
    mock_kafka.send.return_value = future
    p._producer = mock_kafka
    return p


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------


class TestProducerInit:
    def test_default_topics(self):
        p = TradeEventProducer(brokers=["b1:9092"], topic="my.topic")
        assert p.positions_topic == "trading.positions"
        assert p.watchlist_topic == "trading.watchlist"

    def test_custom_topics(self):
        p = TradeEventProducer(
            brokers=["b1:9092"],
            topic="t",
            positions_topic="pos",
            watchlist_topic="wl",
        )
        assert p.positions_topic == "pos"
        assert p.watchlist_topic == "wl"


# ---------------------------------------------------------------------------
# publish_trade
# ---------------------------------------------------------------------------


class TestPublishTrade:
    def test_publishes_to_correct_topic(self, producer):
        trade = _make_trade()
        result = producer.publish_trade(trade)
        assert result is True
        producer._producer.send.assert_called_once()
        args, kwargs = producer._producer.send.call_args
        assert args[0] == "trading.orders"

    def test_uses_symbol_as_key(self, producer):
        trade = _make_trade(symbol="GOOG")
        producer.publish_trade(trade)
        _, kwargs = producer._producer.send.call_args
        assert kwargs["key"] == "GOOG"

    def test_event_structure(self, producer):
        trade = _make_trade()
        producer.publish_trade(trade)
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert event["event_type"] == "TRADE_DETECTED"
        assert event["source"] == "robinhood"
        assert "timestamp" in event
        assert "data" in event
        assert event["data"]["order_id"] == "t-001"

    def test_not_connected_raises(self):
        p = TradeEventProducer(brokers=["b:9092"], topic="t")
        with pytest.raises(RuntimeError, match="not connected"):
            p.publish_trade(_make_trade())

    def test_kafka_error_returns_false(self, producer):
        from kafka.errors import KafkaError

        producer._producer.send.side_effect = KafkaError("boom")
        # reconnect also fails
        with patch.object(producer, "_reconnect_producer", return_value=False):
            result = producer.publish_trade(_make_trade())
        assert result is False


# ---------------------------------------------------------------------------
# publish_trades
# ---------------------------------------------------------------------------


class TestPublishTrades:
    def test_publishes_all(self, producer):
        trades = [_make_trade(order_id=f"t-{i}") for i in range(3)]
        s, f = producer.publish_trades(trades)
        assert s == 3
        assert f == 0

    def test_counts_failures(self, producer):
        trades = [_make_trade(order_id=f"t-{i}") for i in range(3)]
        # Second publish fails
        call_count = {"n": 0}

        def side_effect(trade):
            call_count["n"] += 1
            return call_count["n"] != 2

        with patch.object(producer, "publish_trade", side_effect=side_effect):
            s, f = producer.publish_trades(trades)
        assert s == 2
        assert f == 1

    def test_empty_list(self, producer):
        s, f = producer.publish_trades([])
        assert s == 0
        assert f == 0

    def test_flushes_after_publishing(self, producer):
        producer.publish_trades([_make_trade()])
        producer._producer.flush.assert_called_once()

    def test_not_connected_raises(self):
        p = TradeEventProducer(brokers=["b:9092"], topic="t")
        with pytest.raises(RuntimeError, match="not connected"):
            p.publish_trades([_make_trade()])


# ---------------------------------------------------------------------------
# publish_positions
# ---------------------------------------------------------------------------


class TestPublishPositions:
    def test_publishes_to_positions_topic(self, producer):
        positions = [_make_position()]
        balance = _make_balance()
        result = producer.publish_positions(positions, balance)
        assert result is True
        args, kwargs = producer._producer.send.call_args
        assert args[0] == "trading.positions"

    def test_event_structure(self, producer):
        positions = [_make_position(symbol="AAPL"), _make_position(symbol="GOOG")]
        balance = _make_balance()
        producer.publish_positions(positions, balance)
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert event["event_type"] == "POSITIONS_SNAPSHOT"
        assert len(event["data"]["positions"]) == 2
        assert event["data"]["buying_power"] == "500.00"

    def test_uses_positions_key(self, producer):
        result = producer.publish_positions([_make_position()], _make_balance())
        _, kwargs = producer._producer.send.call_args
        assert kwargs["key"] == "positions"

    def test_not_connected_raises(self):
        p = TradeEventProducer(brokers=["b:9092"], topic="t")
        with pytest.raises(RuntimeError, match="not connected"):
            p.publish_positions([], _make_balance())


# ---------------------------------------------------------------------------
# publish_watchlist_update
# ---------------------------------------------------------------------------


class TestPublishWatchlistUpdate:
    def test_publishes_to_watchlist_topic(self, producer):
        result = producer.publish_watchlist_update(
            added_symbols=["AAPL"],
            removed_symbols=["GOOG"],
            all_symbols=["AAPL", "MSFT"],
        )
        assert result is True
        args, _ = producer._producer.send.call_args
        assert args[0] == "trading.watchlist"

    def test_event_structure(self, producer):
        producer.publish_watchlist_update(
            added_symbols=["AAPL"],
            removed_symbols=["GOOG"],
            all_symbols=["AAPL", "MSFT"],
        )
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert event["event_type"] == "WATCHLIST_UPDATED"
        assert event["data"]["added_symbols"] == ["AAPL"]
        assert event["data"]["removed_symbols"] == ["GOOG"]
        assert event["data"]["total_count"] == 2

    def test_uses_watchlist_key(self, producer):
        producer.publish_watchlist_update([], [], [])
        _, kwargs = producer._producer.send.call_args
        assert kwargs["key"] == "watchlist"

    def test_with_stocks_list(self, producer):
        stocks = [_make_watchlist_stock()]
        producer.publish_watchlist_update(
            added_symbols=["AAPL"],
            removed_symbols=[],
            all_symbols=["AAPL"],
            stocks=stocks,
        )
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert len(event["data"]["stocks"]) == 1


# ---------------------------------------------------------------------------
# publish_symbol_added / publish_symbol_removed
# ---------------------------------------------------------------------------


class TestPublishSymbolEvents:
    def test_symbol_added_event(self, producer):
        result = producer.publish_symbol_added("NVDA", "NVIDIA Corp")
        assert result is True
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert event["event_type"] == "WATCHLIST_SYMBOL_ADDED"
        assert event["data"]["symbol"] == "NVDA"
        assert event["data"]["name"] == "NVIDIA Corp"

    def test_symbol_added_default_name(self, producer):
        producer.publish_symbol_added("NVDA")
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert event["data"]["name"] == "NVDA"

    def test_symbol_added_with_sector(self, producer):
        producer.publish_symbol_added("CCJ", "Cameco", sector="Energy", industry="Uranium")
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert event["data"]["sector"] == "Energy"
        assert event["data"]["industry"] == "Uranium"

    def test_symbol_added_sector_omitted_when_empty(self, producer):
        producer.publish_symbol_added("CCJ", "Cameco")
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert "sector" not in event["data"]
        assert "industry" not in event["data"]

    def test_symbol_added_uses_symbol_as_key(self, producer):
        producer.publish_symbol_added("NVDA")
        _, kwargs = producer._producer.send.call_args
        assert kwargs["key"] == "NVDA"

    def test_symbol_removed_event(self, producer):
        result = producer.publish_symbol_removed("GOOG")
        assert result is True
        _, kwargs = producer._producer.send.call_args
        event = kwargs["value"]
        assert event["event_type"] == "WATCHLIST_SYMBOL_REMOVED"
        assert event["data"]["symbol"] == "GOOG"

    def test_symbol_removed_uses_symbol_as_key(self, producer):
        producer.publish_symbol_removed("GOOG")
        _, kwargs = producer._producer.send.call_args
        assert kwargs["key"] == "GOOG"

    def test_symbol_added_not_connected_raises(self):
        p = TradeEventProducer(brokers=["b:9092"], topic="t")
        with pytest.raises(RuntimeError, match="not connected"):
            p.publish_symbol_added("AAPL")

    def test_symbol_removed_not_connected_raises(self):
        p = TradeEventProducer(brokers=["b:9092"], topic="t")
        with pytest.raises(RuntimeError, match="not connected"):
            p.publish_symbol_removed("AAPL")


# ---------------------------------------------------------------------------
# close / reconnect
# ---------------------------------------------------------------------------


class TestProducerLifecycle:
    def test_close_flushes_and_closes(self, producer):
        producer.close()
        producer._producer.flush.assert_called_once()
        producer._producer.close.assert_called_once()

    def test_close_no_producer(self):
        p = TradeEventProducer(brokers=["b:9092"], topic="t")
        p.close()  # Should not raise

    def test_reconnect_closes_old_producer(self, producer):
        old_producer = producer._producer
        with patch.object(producer, "connect", return_value=True):
            result = producer._reconnect_producer()
        assert result is True
        old_producer.close.assert_called()
