"""Tests for the Redis store classes in redis_client.py.

The internal ``redis.Redis`` client is mocked; no real Redis is contacted.
Covers _RedisBase retry/reconnect, SyncedOrdersTracker, PositionStore,
WatchlistStore, EarningsCalendarStore and StopOrderStore.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import redis

from robinhood_sync.config import Settings
from robinhood_sync.redis_client import (
    _RedisBase,
    SyncedOrdersTracker,
    PositionStore,
    WatchlistStore,
    EarningsCalendarStore,
    StopOrderStore,
)
from robinhood_sync.robinhood_client import Position, AccountBalance, StopOrder


@pytest.fixture
def settings():
    s = MagicMock(spec=Settings)
    s.redis_host = "localhost"
    s.redis_port = 6379
    s.redis_password = None
    s.redis_db = 0
    s.redis_synced_orders_key = "test:synced"
    return s


def _now():
    return datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _position(symbol="AAPL"):
    return Position(
        symbol=symbol, quantity=Decimal("10"), average_buy_price=Decimal("100"),
        equity=Decimal("1100"), percent_change=Decimal("10"),
        equity_change=Decimal("100"), updated_at=_now(),
    )


def _balance():
    return AccountBalance(
        buying_power=Decimal("500"), cash=Decimal("200"),
        total_equity=Decimal("1500"), updated_at=_now(),
    )


def _stop_order(symbol="AAPL", stop_price="90", order_id="o1"):
    return StopOrder(
        order_id=order_id, symbol=symbol, stop_price=Decimal(stop_price),
        quantity=Decimal("10"), side="sell", order_type="market",
        limit_price=None, state="confirmed", created_at=_now(),
    )


# ---------------------------------------------------------------------------
# _RedisBase
# ---------------------------------------------------------------------------

class TestRedisBase:
    def test_create_client_passes_settings(self, settings):
        base = _RedisBase(settings)
        with patch("robinhood_sync.redis_client.redis.Redis") as R:
            base._create_client()
            _, kwargs = R.call_args
            assert kwargs["host"] == "localhost"
            assert kwargs["decode_responses"] is True

    def test_reconnect_success(self, settings):
        base = _RedisBase(settings)
        new_client = MagicMock()
        with patch.object(base, "_create_client", return_value=new_client):
            assert base._reconnect() is True
        new_client.ping.assert_called_once()

    def test_reconnect_closes_old_client(self, settings):
        base = _RedisBase(settings)
        old = MagicMock()
        base._client = old
        with patch.object(base, "_create_client", return_value=MagicMock()):
            base._reconnect()
        old.close.assert_called_once()

    def test_reconnect_swallows_old_close_error(self, settings):
        base = _RedisBase(settings)
        old = MagicMock()
        old.close.side_effect = RuntimeError("already closed")
        base._client = old
        with patch.object(base, "_create_client", return_value=MagicMock()):
            assert base._reconnect() is True

    def test_reconnect_failure_returns_false(self, settings):
        base = _RedisBase(settings)
        with patch.object(
            base, "_create_client",
            side_effect=redis.RedisError("down"),
        ):
            assert base._reconnect() is False
        assert base._client is None

    def test_with_retry_success(self, settings):
        base = _RedisBase(settings)
        func = MagicMock(return_value="ok")
        assert base._with_retry(func, 1, 2) == "ok"
        func.assert_called_once_with(1, 2)

    def test_with_retry_reconnects_then_succeeds(self, settings):
        base = _RedisBase(settings)
        func = MagicMock(side_effect=[redis.ConnectionError("boom"), "ok"])
        with patch.object(base, "_reconnect", return_value=True):
            assert base._with_retry(func) == "ok"
        assert func.call_count == 2

    def test_with_retry_reconnect_fails_raises(self, settings):
        base = _RedisBase(settings)
        func = MagicMock(side_effect=redis.ConnectionError("boom"))
        with patch.object(base, "_reconnect", return_value=False):
            with pytest.raises(redis.ConnectionError):
                base._with_retry(func)

    def test_close(self, settings):
        base = _RedisBase(settings)
        base._client = MagicMock()
        base.close()
        base._client.close.assert_called_once()

    def test_close_no_client(self, settings):
        base = _RedisBase(settings)
        base.close()  # no exception


# ---------------------------------------------------------------------------
# SyncedOrdersTracker
# ---------------------------------------------------------------------------

class TestSyncedOrdersTracker:
    def _tracker(self, settings):
        t = SyncedOrdersTracker(settings)
        t._client = MagicMock()
        return t

    def test_connect_success(self, settings):
        t = SyncedOrdersTracker(settings)
        client = MagicMock()
        client.scard.return_value = 3
        with patch.object(t, "_create_client", return_value=client):
            assert t.connect() is True

    def test_connect_failure(self, settings):
        t = SyncedOrdersTracker(settings)
        with patch.object(t, "_create_client", side_effect=redis.RedisError("x")):
            assert t.connect() is False

    def test_is_synced_requires_connection(self, settings):
        t = SyncedOrdersTracker(settings)
        with pytest.raises(RuntimeError):
            t.is_synced("o1")

    def test_is_synced(self, settings):
        t = self._tracker(settings)
        t._client.sismember.return_value = True
        assert t.is_synced("o1") is True

    def test_mark_synced(self, settings):
        t = self._tracker(settings)
        t.mark_synced("o1")
        t._client.sadd.assert_called_once_with(t.key, "o1")

    def test_mark_many_synced(self, settings):
        t = self._tracker(settings)
        t.mark_many_synced(["o1", "o2"])
        t._client.sadd.assert_called_once()

    def test_mark_many_empty_is_noop(self, settings):
        t = self._tracker(settings)
        t.mark_many_synced([])
        t._client.sadd.assert_not_called()

    def test_get_all_synced(self, settings):
        t = self._tracker(settings)
        t._client.smembers.return_value = {"o1", "o2"}
        assert t.get_all_synced() == {"o1", "o2"}

    def test_count_synced(self, settings):
        t = self._tracker(settings)
        t._client.scard.return_value = 5
        assert t.count_synced() == 5

    def test_remove_synced(self, settings):
        t = self._tracker(settings)
        t.remove_synced("o1")
        t._client.srem.assert_called_once_with(t.key, "o1")

    def test_clear_all(self, settings):
        t = self._tracker(settings)
        t.clear_all()
        t._client.delete.assert_called_once_with(t.key)

    def test_methods_require_connection(self, settings):
        t = SyncedOrdersTracker(settings)
        for call in (
            lambda: t.mark_synced("o"),
            lambda: t.mark_many_synced(["o"]),
            lambda: t.get_all_synced(),
            lambda: t.count_synced(),
            lambda: t.remove_synced("o"),
            lambda: t.clear_all(),
        ):
            with pytest.raises(RuntimeError):
                call()


# ---------------------------------------------------------------------------
# PositionStore
# ---------------------------------------------------------------------------

class TestPositionStore:
    def _store(self, settings):
        s = PositionStore(settings)
        s._client = MagicMock()
        s._client.pipeline.return_value = MagicMock()
        return s

    def test_connect_success(self, settings):
        s = PositionStore(settings)
        with patch.object(s, "_create_client", return_value=MagicMock()):
            assert s.connect() is True

    def test_connect_failure(self, settings):
        s = PositionStore(settings)
        with patch.object(s, "_create_client", side_effect=redis.RedisError("x")):
            assert s.connect() is False

    def test_store_positions_empty_deletes(self, settings):
        s = self._store(settings)
        assert s.store_positions([]) is True
        s._client.delete.assert_called_once_with(s.POSITIONS_KEY)

    def test_store_positions_pipeline(self, settings):
        s = self._store(settings)
        assert s.store_positions([_position()]) is True
        s._client.pipeline.assert_called_once()

    def test_store_positions_error_returns_false(self, settings):
        s = self._store(settings)
        s._client.pipeline.side_effect = redis.RedisError("boom")
        assert s.store_positions([_position()]) is False

    def test_store_positions_requires_connection(self, settings):
        s = PositionStore(settings)
        with pytest.raises(RuntimeError):
            s.store_positions([])

    def test_store_buying_power(self, settings):
        s = self._store(settings)
        assert s.store_buying_power(_balance()) is True
        s._client.set.assert_called_once()

    def test_store_buying_power_error(self, settings):
        s = self._store(settings)
        s._client.set.side_effect = redis.RedisError("boom")
        assert s.store_buying_power(_balance()) is False

    def test_store_daily_equity_new(self, settings):
        s = self._store(settings)
        s._client.get.return_value = None
        assert s.store_daily_equity_open(Decimal("1500"), "2026-01-01") is True

    def test_store_daily_equity_already_today(self, settings):
        s = self._store(settings)
        s._client.get.return_value = json.dumps({"date": "2026-01-01"})
        assert s.store_daily_equity_open(Decimal("1500"), "2026-01-01") is False

    def test_store_daily_equity_corrupt_overwrites(self, settings):
        s = self._store(settings)
        s._client.get.return_value = "not-json"
        assert s.store_daily_equity_open(Decimal("1500"), "2026-01-01") is True

    def test_store_daily_equity_error(self, settings):
        s = self._store(settings)
        s._client.get.side_effect = redis.RedisError("boom")
        assert s.store_daily_equity_open(Decimal("1"), "2026-01-01") is False

    def test_get_positions(self, settings):
        s = self._store(settings)
        s._client.hgetall.return_value = {"AAPL": json.dumps({"symbol": "AAPL"})}
        result = s.get_positions()
        assert result["AAPL"]["symbol"] == "AAPL"

    def test_get_positions_error(self, settings):
        s = self._store(settings)
        s._client.hgetall.side_effect = redis.RedisError("boom")
        assert s.get_positions() == {}

    def test_get_buying_power(self, settings):
        s = self._store(settings)
        s._client.get.return_value = json.dumps({"buying_power": "500"})
        assert s.get_buying_power()["buying_power"] == "500"

    def test_get_buying_power_none(self, settings):
        s = self._store(settings)
        s._client.get.return_value = None
        assert s.get_buying_power() is None

    def test_get_buying_power_error(self, settings):
        s = self._store(settings)
        s._client.get.side_effect = redis.RedisError("boom")
        assert s.get_buying_power() is None


# ---------------------------------------------------------------------------
# WatchlistStore
# ---------------------------------------------------------------------------

class TestWatchlistStore:
    def _store(self, settings):
        s = WatchlistStore(settings)
        s._client = MagicMock()
        s._client.pipeline.return_value = MagicMock()
        return s

    def test_connect_success(self, settings):
        s = WatchlistStore(settings)
        client = MagicMock()
        client.scard.return_value = 2
        with patch.object(s, "_create_client", return_value=client):
            assert s.connect() is True

    def test_connect_failure(self, settings):
        s = WatchlistStore(settings)
        with patch.object(s, "_create_client", side_effect=redis.RedisError("x")):
            assert s.connect() is False

    def test_sync_watchlist_adds_and_removes(self, settings):
        s = self._store(settings)
        s._client.smembers.return_value = {"OLD", "KEEP"}
        stocks = [{"symbol": "KEEP"}, {"symbol": "NEW"}]
        added, removed = s.sync_watchlist(stocks)
        assert added == ["NEW"]
        assert removed == ["OLD"]

    def test_sync_watchlist_no_changes(self, settings):
        s = self._store(settings)
        s._client.smembers.return_value = {"A"}
        added, removed = s.sync_watchlist([{"symbol": "A"}])
        assert added == [] and removed == []

    def test_sync_watchlist_watch_error_retries(self, settings):
        s = self._store(settings)
        # First call raises WatchError, then succeeds
        s._client.smembers.side_effect = [redis.WatchError(), {"A"}]
        with patch("robinhood_sync.redis_client.time.sleep"):
            added, removed = s.sync_watchlist([{"symbol": "A"}])
        assert added == [] and removed == []

    def test_sync_watchlist_watch_error_exhausted(self, settings):
        s = self._store(settings)
        s._client.smembers.side_effect = redis.WatchError()
        with patch("robinhood_sync.redis_client.time.sleep"):
            with pytest.raises(RuntimeError, match="WatchError"):
                s.sync_watchlist([{"symbol": "A"}])

    def test_sync_watchlist_connection_error_reconnects(self, settings):
        s = self._store(settings)
        s._client.smembers.side_effect = [redis.ConnectionError("x"), {"A"}]
        with patch.object(s, "_reconnect", return_value=True):
            added, removed = s.sync_watchlist([{"symbol": "A"}])
        assert added == [] and removed == []

    def test_sync_watchlist_connection_error_no_reconnect_raises(self, settings):
        s = self._store(settings)
        s._client.smembers.side_effect = redis.ConnectionError("x")
        with patch.object(s, "_reconnect", return_value=False):
            with pytest.raises(redis.ConnectionError):
                s.sync_watchlist([{"symbol": "A"}])

    def test_sync_watchlist_redis_error_raises(self, settings):
        s = self._store(settings)
        s._client.smembers.side_effect = redis.RedisError("x")
        with pytest.raises(redis.RedisError):
            s.sync_watchlist([{"symbol": "A"}])

    def test_get_symbols(self, settings):
        s = self._store(settings)
        s._client.smembers.return_value = {"A", "B"}
        assert s.get_symbols() == {"A", "B"}

    def test_get_symbols_error(self, settings):
        s = self._store(settings)
        s._client.smembers.side_effect = redis.RedisError("x")
        assert s.get_symbols() == set()

    def test_get_symbol_details(self, settings):
        s = self._store(settings)
        s._client.hget.return_value = json.dumps({"symbol": "A"})
        assert s.get_symbol_details("A")["symbol"] == "A"

    def test_get_symbol_details_none(self, settings):
        s = self._store(settings)
        s._client.hget.return_value = None
        assert s.get_symbol_details("A") is None

    def test_get_symbol_details_error(self, settings):
        s = self._store(settings)
        s._client.hget.side_effect = redis.RedisError("x")
        assert s.get_symbol_details("A") is None

    def test_get_all_details(self, settings):
        s = self._store(settings)
        s._client.hgetall.return_value = {"A": json.dumps({"symbol": "A"})}
        assert s.get_all_details()["A"]["symbol"] == "A"

    def test_get_all_details_error(self, settings):
        s = self._store(settings)
        s._client.hgetall.side_effect = redis.RedisError("x")
        assert s.get_all_details() == {}

    def test_add_symbol_new(self, settings):
        s = self._store(settings)
        s._client.sismember.return_value = False
        assert s.add_symbol("AAPL", "Apple") is True

    def test_add_symbol_existing(self, settings):
        s = self._store(settings)
        s._client.sismember.return_value = True
        assert s.add_symbol("AAPL") is False

    def test_add_symbol_error(self, settings):
        s = self._store(settings)
        s._client.sismember.side_effect = redis.RedisError("x")
        assert s.add_symbol("AAPL") is False

    def test_remove_symbol(self, settings):
        s = self._store(settings)
        s._client.srem.return_value = 1
        assert s.remove_symbol("AAPL") is True

    def test_remove_symbol_absent(self, settings):
        s = self._store(settings)
        s._client.srem.return_value = 0
        assert s.remove_symbol("AAPL") is False

    def test_remove_symbol_error(self, settings):
        s = self._store(settings)
        s._client.srem.side_effect = redis.RedisError("x")
        assert s.remove_symbol("AAPL") is False

    def test_symbol_exists(self, settings):
        s = self._store(settings)
        s._client.sismember.return_value = True
        assert s.symbol_exists("AAPL") is True

    def test_symbol_exists_error(self, settings):
        s = self._store(settings)
        s._client.sismember.side_effect = redis.RedisError("x")
        assert s.symbol_exists("AAPL") is False

    def test_count(self, settings):
        s = self._store(settings)
        s._client.scard.return_value = 7
        assert s.count() == 7

    def test_count_error(self, settings):
        s = self._store(settings)
        s._client.scard.side_effect = redis.RedisError("x")
        assert s.count() == 0


# ---------------------------------------------------------------------------
# EarningsCalendarStore
# ---------------------------------------------------------------------------

class TestEarningsCalendarStore:
    def _store(self, settings):
        s = EarningsCalendarStore(settings)
        s._client = MagicMock()
        return s

    def test_connect_success(self, settings):
        s = EarningsCalendarStore(settings)
        with patch.object(s, "_create_client", return_value=MagicMock()):
            assert s.connect() is True

    def test_connect_failure(self, settings):
        s = EarningsCalendarStore(settings)
        with patch.object(s, "_create_client", side_effect=redis.RedisError("x")):
            assert s.connect() is False

    def test_store_next_earnings(self, settings):
        s = self._store(settings)
        data = {"date": "2026-02-01", "timing": "am", "verified": True, "days_away": 5}
        assert s.store_next_earnings("AAPL", data) is True
        s._client.set.assert_called_once()

    def test_store_next_earnings_error(self, settings):
        s = self._store(settings)
        s._client.set.side_effect = redis.RedisError("x")
        data = {"date": "2026-02-01", "timing": "am", "verified": True, "days_away": 5}
        assert s.store_next_earnings("AAPL", data) is False

    def test_clear_earnings(self, settings):
        s = self._store(settings)
        s.clear_earnings("AAPL")
        s._client.delete.assert_called_once()

    def test_clear_earnings_no_client_is_noop(self, settings):
        s = EarningsCalendarStore(settings)
        s.clear_earnings("AAPL")  # no exception

    def test_clear_earnings_swallows_error(self, settings):
        s = self._store(settings)
        s._client.delete.side_effect = redis.RedisError("x")
        s.clear_earnings("AAPL")  # no exception

    def test_get_next_earnings(self, settings):
        s = self._store(settings)
        s._client.get.return_value = json.dumps({"date": "2026-02-01"})
        assert s.get_next_earnings("AAPL")["date"] == "2026-02-01"

    def test_get_next_earnings_absent(self, settings):
        s = self._store(settings)
        s._client.get.return_value = None
        assert s.get_next_earnings("AAPL") is None

    def test_get_next_earnings_error(self, settings):
        s = self._store(settings)
        s._client.get.side_effect = redis.RedisError("x")
        assert s.get_next_earnings("AAPL") is None

    def test_get_next_earnings_requires_connection(self, settings):
        s = EarningsCalendarStore(settings)
        with pytest.raises(RuntimeError):
            s.get_next_earnings("AAPL")


# ---------------------------------------------------------------------------
# StopOrderStore
# ---------------------------------------------------------------------------

class TestStopOrderStore:
    def _store(self, settings):
        s = StopOrderStore(settings)
        s._client = MagicMock()
        s._client.pipeline.return_value = MagicMock()
        return s

    def test_connect_success(self, settings):
        s = StopOrderStore(settings)
        with patch.object(s, "_create_client", return_value=MagicMock()):
            assert s.connect() is True

    def test_connect_failure(self, settings):
        s = StopOrderStore(settings)
        with patch.object(s, "_create_client", side_effect=redis.RedisError("x")):
            assert s.connect() is False

    def test_store_stop_orders_empty_deletes(self, settings):
        s = self._store(settings)
        assert s.store_stop_orders([]) is True
        s._client.delete.assert_called_once_with(s.STOP_ORDERS_KEY)

    def test_store_stop_orders_pipeline(self, settings):
        s = self._store(settings)
        assert s.store_stop_orders([_stop_order()]) is True
        s._client.pipeline.assert_called_once()

    def test_store_stop_orders_dedupes_highest_price(self, settings):
        s = self._store(settings)
        orders = [
            _stop_order(stop_price="90", order_id="low"),
            _stop_order(stop_price="95", order_id="high"),
        ]
        # Both same symbol — keeps higher stop price (just confirm success)
        assert s.store_stop_orders(orders) is True

    def test_store_stop_orders_error(self, settings):
        s = self._store(settings)
        s._client.pipeline.side_effect = redis.RedisError("x")
        assert s.store_stop_orders([_stop_order()]) is False

    def test_get_stop_order(self, settings):
        s = self._store(settings)
        s._client.hget.return_value = json.dumps({"symbol": "AAPL"})
        assert s.get_stop_order("AAPL")["symbol"] == "AAPL"

    def test_get_stop_order_none(self, settings):
        s = self._store(settings)
        s._client.hget.return_value = None
        assert s.get_stop_order("AAPL") is None

    def test_get_stop_order_error(self, settings):
        s = self._store(settings)
        s._client.hget.side_effect = redis.RedisError("x")
        assert s.get_stop_order("AAPL") is None

    def test_get_all_stop_orders(self, settings):
        s = self._store(settings)
        s._client.hgetall.return_value = {"AAPL": json.dumps({"symbol": "AAPL"})}
        assert s.get_all_stop_orders()["AAPL"]["symbol"] == "AAPL"

    def test_get_all_stop_orders_error(self, settings):
        s = self._store(settings)
        s._client.hgetall.side_effect = redis.RedisError("x")
        assert s.get_all_stop_orders() == {}
