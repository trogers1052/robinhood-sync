"""Tests for data model dataclasses — to_dict serialization, field types, edge cases."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from robinhood_sync.robinhood_client import (
    Trade,
    Position,
    AccountBalance,
    WatchlistStock,
    StopOrder,
)


# ---------------------------------------------------------------------------
# Trade.to_dict
# ---------------------------------------------------------------------------


class TestTradeToDict:
    def _make_trade(self, **overrides):
        defaults = dict(
            order_id="abc-123",
            symbol="AAPL",
            side="buy",
            quantity=Decimal("10"),
            average_price=Decimal("150.50"),
            total_notional=Decimal("1505.00"),
            fees=Decimal("0.00"),
            state="filled",
            executed_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
            created_at=datetime(2026, 2, 20, 14, 0, tzinfo=timezone.utc),
            instrument_url="https://api.robinhood.com/instruments/1/",
        )
        defaults.update(overrides)
        return Trade(**defaults)

    def test_basic_fields(self):
        t = self._make_trade()
        d = t.to_dict()
        assert d["order_id"] == "abc-123"
        assert d["symbol"] == "AAPL"
        assert d["side"] == "buy"
        assert d["state"] == "filled"

    def test_decimal_fields_serialized_as_strings(self):
        t = self._make_trade()
        d = t.to_dict()
        assert d["quantity"] == "10"
        assert d["average_price"] == "150.50"
        assert d["total_notional"] == "1505.00"
        assert d["fees"] == "0.00"

    def test_executed_at_isoformat(self):
        t = self._make_trade()
        d = t.to_dict()
        assert "2026-02-20" in d["executed_at"]

    def test_executed_at_none(self):
        t = self._make_trade(executed_at=None)
        d = t.to_dict()
        assert d["executed_at"] is None

    def test_created_at_isoformat(self):
        t = self._make_trade()
        d = t.to_dict()
        assert "2026-02-20" in d["created_at"]

    def test_instrument_url_not_in_dict(self):
        """instrument_url is not serialized to Kafka."""
        t = self._make_trade()
        d = t.to_dict()
        assert "instrument_url" not in d

    def test_zero_quantity(self):
        t = self._make_trade(quantity=Decimal("0"))
        d = t.to_dict()
        assert d["quantity"] == "0"

    def test_large_notional(self):
        t = self._make_trade(total_notional=Decimal("1234567.89"))
        d = t.to_dict()
        assert d["total_notional"] == "1234567.89"


# ---------------------------------------------------------------------------
# Position.to_dict
# ---------------------------------------------------------------------------


class TestPositionToDict:
    def _make_position(self, **overrides):
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

    def test_basic_fields(self):
        p = self._make_position()
        d = p.to_dict()
        assert d["symbol"] == "AAPL"
        assert d["quantity"] == "10"

    def test_decimal_fields(self):
        p = self._make_position()
        d = p.to_dict()
        assert d["average_buy_price"] == "150.00"
        assert d["equity"] == "1600.00"
        assert d["percent_change"] == "6.67"
        assert d["equity_change"] == "100.00"

    def test_updated_at_isoformat(self):
        p = self._make_position()
        d = p.to_dict()
        assert "2026-02-20" in d["updated_at"]

    def test_negative_change(self):
        p = self._make_position(
            percent_change=Decimal("-10.5"),
            equity_change=Decimal("-157.50"),
        )
        d = p.to_dict()
        assert d["percent_change"] == "-10.5"
        assert d["equity_change"] == "-157.50"

    def test_zero_equity(self):
        p = self._make_position(equity=Decimal("0"), equity_change=Decimal("0"))
        d = p.to_dict()
        assert d["equity"] == "0"


# ---------------------------------------------------------------------------
# AccountBalance.to_dict
# ---------------------------------------------------------------------------


class TestAccountBalanceToDict:
    def _make_balance(self, **overrides):
        defaults = dict(
            buying_power=Decimal("500.00"),
            cash=Decimal("300.00"),
            total_equity=Decimal("1200.00"),
            updated_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
        )
        defaults.update(overrides)
        return AccountBalance(**defaults)

    def test_basic_fields(self):
        b = self._make_balance()
        d = b.to_dict()
        assert d["buying_power"] == "500.00"
        assert d["cash"] == "300.00"
        assert d["total_equity"] == "1200.00"

    def test_updated_at_isoformat(self):
        b = self._make_balance()
        d = b.to_dict()
        assert "2026-02-20" in d["updated_at"]

    def test_zero_balance(self):
        b = self._make_balance(
            buying_power=Decimal("0"),
            cash=Decimal("0"),
            total_equity=Decimal("0"),
        )
        d = b.to_dict()
        assert d["buying_power"] == "0"


# ---------------------------------------------------------------------------
# WatchlistStock.to_dict
# ---------------------------------------------------------------------------


class TestWatchlistStockToDict:
    def _make_stock(self, **overrides):
        defaults = dict(
            symbol="AAPL",
            name="Apple Inc.",
            instrument_url="https://api.robinhood.com/instruments/1/",
            added_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
        )
        defaults.update(overrides)
        return WatchlistStock(**defaults)

    def test_basic_fields(self):
        s = self._make_stock()
        d = s.to_dict()
        assert d["symbol"] == "AAPL"
        assert d["name"] == "Apple Inc."
        assert d["instrument_url"] == "https://api.robinhood.com/instruments/1/"

    def test_added_at_isoformat(self):
        s = self._make_stock()
        d = s.to_dict()
        assert "2026-02-20" in d["added_at"]

    def test_empty_instrument_url(self):
        s = self._make_stock(instrument_url="")
        d = s.to_dict()
        assert d["instrument_url"] == ""

    def test_sector_included_when_present(self):
        s = self._make_stock(sector="Technology")
        d = s.to_dict()
        assert d["sector"] == "Technology"

    def test_industry_included_when_present(self):
        s = self._make_stock(industry="Consumer Electronics")
        d = s.to_dict()
        assert d["industry"] == "Consumer Electronics"

    def test_sector_and_industry_both_present(self):
        s = self._make_stock(sector="Energy", industry="Uranium")
        d = s.to_dict()
        assert d["sector"] == "Energy"
        assert d["industry"] == "Uranium"

    def test_sector_omitted_when_none(self):
        s = self._make_stock()
        d = s.to_dict()
        assert "sector" not in d
        assert "industry" not in d

    def test_sector_omitted_when_empty_string(self):
        s = self._make_stock(sector="", industry="")
        d = s.to_dict()
        assert "sector" not in d
        assert "industry" not in d


# ---------------------------------------------------------------------------
# StopOrder.to_dict
# ---------------------------------------------------------------------------


class TestStopOrderToDict:
    def _make_stop(self, **overrides):
        defaults = dict(
            order_id="stop-123",
            symbol="AAPL",
            stop_price=Decimal("140.00"),
            quantity=Decimal("10"),
            side="sell",
            order_type="market",
            limit_price=None,
            state="confirmed",
            created_at=datetime(2026, 2, 20, 14, 30, tzinfo=timezone.utc),
        )
        defaults.update(overrides)
        return StopOrder(**defaults)

    def test_basic_fields(self):
        s = self._make_stop()
        d = s.to_dict()
        assert d["order_id"] == "stop-123"
        assert d["symbol"] == "AAPL"
        assert d["stop_price"] == "140.00"
        assert d["quantity"] == "10"
        assert d["side"] == "sell"
        assert d["order_type"] == "market"
        assert d["state"] == "confirmed"

    def test_limit_price_none(self):
        s = self._make_stop(limit_price=None)
        d = s.to_dict()
        assert d["limit_price"] is None

    def test_limit_price_present(self):
        s = self._make_stop(limit_price=Decimal("139.50"), order_type="limit")
        d = s.to_dict()
        assert d["limit_price"] == "139.50"
        assert d["order_type"] == "limit"

    def test_created_at_isoformat(self):
        s = self._make_stop()
        d = s.to_dict()
        assert "2026-02-20" in d["created_at"]
