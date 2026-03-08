"""Tests for RobinhoodClient._parse_order — the most critical parsing logic."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from robinhood_sync.robinhood_client import RobinhoodClient


@pytest.fixture
def client():
    c = RobinhoodClient("user", "pass")
    c._logged_in = True
    # Patch instrument lookup to avoid real API calls
    c._get_symbol_from_instrument = lambda url: "AAPL"
    return c


def _minimal_order(**overrides):
    """Build a minimal valid Robinhood order dict."""
    base = {
        "id": "order-001",
        "instrument": "https://api.robinhood.com/instruments/1/",
        "side": "buy",
        "cumulative_quantity": "10.00000000",
        "average_price": "150.50000000",
        "executed_notional": {"amount": "1505.00"},
        "fees": "0.00",
        "state": "filled",
        "executions": [{"timestamp": "2026-02-20T14:30:00Z"}],
        "created_at": "2026-02-20T14:00:00Z",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Successful parsing
# ---------------------------------------------------------------------------


class TestParseOrderSuccess:
    def test_basic_order(self, client):
        trade = client._parse_order(_minimal_order())
        assert trade is not None
        assert trade.order_id == "order-001"
        assert trade.symbol == "AAPL"
        assert trade.side == "buy"
        assert trade.quantity == Decimal("10.00000000")
        assert trade.average_price == Decimal("150.50000000")
        assert trade.state == "filled"

    def test_sell_side(self, client):
        trade = client._parse_order(_minimal_order(side="sell"))
        assert trade.side == "sell"

    def test_executed_at_from_executions(self, client):
        trade = client._parse_order(_minimal_order())
        assert trade.executed_at is not None
        assert trade.executed_at.year == 2026
        assert trade.executed_at.month == 2

    def test_executed_at_from_last_execution(self, client):
        """Uses the LAST execution timestamp when there are multiple."""
        order = _minimal_order(executions=[
            {"timestamp": "2026-01-01T10:00:00Z"},
            {"timestamp": "2026-02-20T14:30:00Z"},
        ])
        trade = client._parse_order(order)
        assert trade.executed_at.month == 2

    def test_total_notional_from_executed_notional(self, client):
        trade = client._parse_order(_minimal_order())
        assert trade.total_notional == Decimal("1505.00")

    def test_total_notional_calculated_when_zero(self, client):
        """When executed_notional amount is 0, falls back to qty * price."""
        order = _minimal_order(executed_notional={"amount": "0"})
        trade = client._parse_order(order)
        expected = Decimal("10.00000000") * Decimal("150.50000000")
        assert trade.total_notional == expected

    def test_total_notional_calculated_when_none(self, client):
        """When executed_notional is None, falls back to qty * price."""
        order = _minimal_order(executed_notional=None)
        trade = client._parse_order(order)
        expected = Decimal("10.00000000") * Decimal("150.50000000")
        assert trade.total_notional == expected

    def test_fees_parsed(self, client):
        trade = client._parse_order(_minimal_order(fees="1.50"))
        assert trade.fees == Decimal("1.50")


# ---------------------------------------------------------------------------
# Fallback paths
# ---------------------------------------------------------------------------


class TestParseOrderFallbacks:
    def test_executed_at_fallback_to_last_transaction_at(self, client):
        order = _minimal_order(executions=[], last_transaction_at="2026-02-19T12:00:00Z")
        trade = client._parse_order(order)
        assert trade.executed_at is not None
        assert trade.executed_at.day == 19

    def test_executed_at_none_when_no_timestamps(self, client):
        order = _minimal_order(executions=[])
        order.pop("last_transaction_at", None)  # ensure not present
        trade = client._parse_order(order)
        assert trade.executed_at is None

    def test_created_at_fallback_to_now(self, client):
        order = _minimal_order(created_at=None)
        trade = client._parse_order(order)
        assert trade.created_at is not None
        # Should be close to now
        assert (datetime.now() - trade.created_at).total_seconds() < 5

    def test_side_defaults_to_empty_on_none(self, client):
        order = _minimal_order(side=None)
        trade = client._parse_order(order)
        assert trade.side == ""

    def test_state_defaults_to_unknown_on_none(self, client):
        order = _minimal_order(state=None)
        trade = client._parse_order(order)
        assert trade.state == "unknown"


# ---------------------------------------------------------------------------
# Null / missing field handling
# ---------------------------------------------------------------------------


class TestParseOrderNullFields:
    def test_none_cumulative_quantity(self, client):
        order = _minimal_order(cumulative_quantity=None)
        trade = client._parse_order(order)
        assert trade.quantity == Decimal("0")

    def test_none_average_price(self, client):
        order = _minimal_order(average_price=None)
        trade = client._parse_order(order)
        assert trade.average_price == Decimal("0")

    def test_none_fees(self, client):
        order = _minimal_order(fees=None)
        trade = client._parse_order(order)
        assert trade.fees == Decimal("0")

    def test_executed_notional_is_string(self, client):
        """Handles case where executed_notional is not a dict."""
        order = _minimal_order(executed_notional="not_a_dict")
        trade = client._parse_order(order)
        # Should fall back to qty * price
        assert trade.total_notional == trade.quantity * trade.average_price

    def test_executed_notional_missing_amount(self, client):
        order = _minimal_order(executed_notional={"currency": "USD"})
        trade = client._parse_order(order)
        # amount is None, should default to "0" -> fall back to qty * price
        assert trade.total_notional == trade.quantity * trade.average_price

    def test_executions_with_none_entry(self, client):
        """Handles None in the executions list."""
        order = _minimal_order(executions=[None])
        trade = client._parse_order(order)
        # Should not crash, executed_at will be None (falls back to last_transaction_at or None)
        assert trade is not None

    def test_executions_with_empty_dict(self, client):
        """Handles empty dict in the executions list."""
        order = _minimal_order(executions=[{}])
        trade = client._parse_order(order)
        assert trade is not None

    def test_executions_not_a_list(self, client):
        """Handles executions being a non-list value."""
        order = _minimal_order(executions="not_a_list")
        trade = client._parse_order(order)
        # Should handle gracefully - executions check will fail the isinstance check
        assert trade is not None


# ---------------------------------------------------------------------------
# Rejection paths (returns None)
# ---------------------------------------------------------------------------


class TestParseOrderReturnsNone:
    def test_none_order(self, client):
        assert client._parse_order(None) is None

    def test_non_dict_order(self, client):
        assert client._parse_order("not a dict") is None

    def test_empty_dict(self, client):
        assert client._parse_order({}) is None

    def test_no_order_id(self, client):
        order = _minimal_order()
        del order["id"]
        assert client._parse_order(order) is None

    def test_no_instrument_url(self, client):
        order = _minimal_order(instrument="")
        assert client._parse_order(order) is None

    def test_instrument_none(self, client):
        order = _minimal_order(instrument=None)
        assert client._parse_order(order) is None


# ---------------------------------------------------------------------------
# Side case-insensitivity
# ---------------------------------------------------------------------------


class TestParseOrderSideCasing:
    def test_uppercase_buy(self, client):
        trade = client._parse_order(_minimal_order(side="BUY"))
        assert trade.side == "buy"

    def test_uppercase_sell(self, client):
        trade = client._parse_order(_minimal_order(side="SELL"))
        assert trade.side == "sell"

    def test_mixed_case(self, client):
        trade = client._parse_order(_minimal_order(side="Buy"))
        assert trade.side == "buy"
