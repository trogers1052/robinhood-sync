"""Additional RobinhoodClient tests for the data-fetch API methods.

All robin_stocks calls go through the patched ``rh`` module — no network.
"""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from robinhood_sync.robinhood_client import RobinhoodClient


def _client():
    c = RobinhoodClient("user", "pass")
    c._logged_in = True
    c._last_api_call = 0.0
    return c


# ---------------------------------------------------------------------------
# get_all_orders / get_filled_orders
# ---------------------------------------------------------------------------

class TestGetAllOrders:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_parses_orders(self, mock_rh):
        c = _client()
        order = {
            "id": "o1", "instrument": "url1", "side": "buy",
            "cumulative_quantity": "10", "average_price": "100",
            "executed_notional": "1000", "fees": "0", "state": "filled",
            "last_transaction_at": "2026-01-01T10:00:00Z",
            "created_at": "2026-01-01T09:00:00Z",
        }
        mock_rh.orders.get_all_stock_orders.return_value = [order]
        with patch.object(c, "_get_symbol_from_instrument", return_value="AAPL"):
            trades = c.get_all_orders()
        assert len(trades) == 1
        assert trades[0].symbol == "AAPL"

    @patch("robinhood_sync.robinhood_client.rh")
    def test_api_error_raises(self, mock_rh):
        c = _client()
        mock_rh.orders.get_all_stock_orders.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            c.get_all_orders()

    @patch("robinhood_sync.robinhood_client.rh")
    def test_filled_orders_filters_and_sorts(self, mock_rh):
        c = _client()
        filled = {
            "id": "f", "instrument": "u", "side": "buy",
            "cumulative_quantity": "1", "average_price": "10",
            "executed_notional": "10", "fees": "0", "state": "filled",
            "last_transaction_at": "2026-01-02T10:00:00Z",
            "created_at": "2026-01-02T09:00:00Z",
        }
        unfilled = dict(filled, id="u1", state="cancelled")
        mock_rh.orders.get_all_stock_orders.return_value = [filled, unfilled]
        with patch.object(c, "_get_symbol_from_instrument", return_value="AAPL"):
            trades = c.get_filled_orders()
        assert all(t.state == "filled" for t in trades)
        assert len(trades) == 1

    @patch("robinhood_sync.robinhood_client.rh")
    def test_filled_orders_since_days(self, mock_rh):
        c = _client()
        old = {
            "id": "old", "instrument": "u", "side": "buy",
            "cumulative_quantity": "1", "average_price": "10",
            "executed_notional": "10", "fees": "0", "state": "filled",
            "last_transaction_at": "2020-01-01T10:00:00Z",
            "created_at": "2020-01-01T09:00:00Z",
        }
        recent = dict(old, id="new",
                      last_transaction_at=datetime.now(timezone.utc).isoformat())
        mock_rh.orders.get_all_stock_orders.return_value = [old, recent]
        with patch.object(c, "_get_symbol_from_instrument", return_value="AAPL"):
            trades = c.get_filled_orders(since_days=7)
        assert len(trades) == 1
        assert trades[0].order_id == "new"


# ---------------------------------------------------------------------------
# get_current_positions
# ---------------------------------------------------------------------------

class TestGetCurrentPositions:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_parses_holdings(self, mock_rh):
        c = _client()
        mock_rh.account.build_holdings.return_value = {
            "AAPL": {
                "quantity": "10", "average_buy_price": "100",
                "equity": "1100", "percent_change": "10", "equity_change": "100",
            }
        }
        positions = c.get_current_positions()
        assert len(positions) == 1
        assert positions[0].symbol == "AAPL"

    @patch("robinhood_sync.robinhood_client.rh")
    def test_empty_holdings(self, mock_rh):
        c = _client()
        mock_rh.account.build_holdings.return_value = {}
        assert c.get_current_positions() == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_bad_holding_skipped(self, mock_rh):
        c = _client()
        mock_rh.account.build_holdings.return_value = {
            "AAPL": {"quantity": "bad"},
            "MSFT": {
                "quantity": "5", "average_buy_price": "200",
                "equity": "1000", "percent_change": "0", "equity_change": "0",
            },
        }
        positions = c.get_current_positions()
        symbols = {p.symbol for p in positions}
        assert "MSFT" in symbols
        assert "AAPL" not in symbols

    @patch("robinhood_sync.robinhood_client.rh")
    def test_error_raises(self, mock_rh):
        c = _client()
        mock_rh.account.build_holdings.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            c.get_current_positions()


# ---------------------------------------------------------------------------
# get_account_balance
# ---------------------------------------------------------------------------

class TestGetAccountBalance:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_parses_balance(self, mock_rh):
        c = _client()
        mock_rh.profiles.load_account_profile.return_value = {
            "buying_power": "500", "cash": "200",
        }
        mock_rh.profiles.load_portfolio_profile.return_value = {"equity": "1500"}
        bal = c.get_account_balance()
        assert bal.buying_power == Decimal("500")
        assert bal.total_equity == Decimal("1500")

    @patch("robinhood_sync.robinhood_client.rh")
    def test_no_profile_raises(self, mock_rh):
        c = _client()
        mock_rh.profiles.load_account_profile.return_value = None
        with pytest.raises(RuntimeError):
            c.get_account_balance()

    @patch("robinhood_sync.robinhood_client.rh")
    def test_no_portfolio_zero_equity(self, mock_rh):
        c = _client()
        mock_rh.profiles.load_account_profile.return_value = {
            "buying_power": "500", "cash": "200",
        }
        mock_rh.profiles.load_portfolio_profile.return_value = None
        bal = c.get_account_balance()
        assert bal.total_equity == Decimal("0")


# ---------------------------------------------------------------------------
# get_watchlist
# ---------------------------------------------------------------------------

class TestGetWatchlist:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_no_watchlists(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = []
        assert c.get_watchlist("Default") == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_watchlist_not_found(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = [
            {"display_name": "Other"}
        ]
        assert c.get_watchlist("Default") == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_empty_items(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = [
            {"display_name": "Default"}
        ]
        mock_rh.account.get_watchlist_by_name.return_value = []
        assert c.get_watchlist("Default") == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_items_with_instrument_url(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = [
            {"display_name": "Default"}
        ]
        mock_rh.account.get_watchlist_by_name.return_value = [
            {"instrument": "http://inst/1", "created_at": "2026-01-01T00:00:00Z"}
        ]
        mock_rh.stocks.get_instrument_by_url.return_value = {
            "symbol": "AAPL", "simple_name": "Apple"
        }
        stocks = c.get_watchlist("Default")
        assert stocks[0].symbol == "AAPL"

    @patch("robinhood_sync.robinhood_client.rh")
    def test_items_as_symbol_strings(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = [
            {"display_name": "Default"}
        ]
        mock_rh.account.get_watchlist_by_name.return_value = ["AAPL", "MSFT"]
        stocks = c.get_watchlist("Default")
        symbols = {s.symbol for s in stocks}
        assert symbols == {"AAPL", "MSFT"}

    @patch("robinhood_sync.robinhood_client.rh")
    def test_string_watchlist_format(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = ["Default"]
        mock_rh.account.get_watchlist_by_name.return_value = []
        assert c.get_watchlist("Default") == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_error_raises(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            c.get_watchlist("Default")


# ---------------------------------------------------------------------------
# get_fundamentals
# ---------------------------------------------------------------------------

class TestGetFundamentals:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_empty_symbols(self, mock_rh):
        c = _client()
        assert c.get_fundamentals([]) == {}

    @patch("robinhood_sync.robinhood_client.rh")
    def test_parses_fundamentals(self, mock_rh):
        c = _client()
        mock_rh.stocks.get_fundamentals.return_value = [
            {"sector": "Tech", "industry": "Hardware"},
        ]
        result = c.get_fundamentals(["AAPL"])
        assert result["AAPL"]["sector"] == "Tech"

    @patch("robinhood_sync.robinhood_client.rh")
    def test_skips_bad_entries(self, mock_rh):
        c = _client()
        mock_rh.stocks.get_fundamentals.return_value = [None, "not-dict"]
        assert c.get_fundamentals(["AAPL", "MSFT"]) == {}

    @patch("robinhood_sync.robinhood_client.rh")
    def test_error_returns_empty(self, mock_rh):
        c = _client()
        mock_rh.stocks.get_fundamentals.side_effect = RuntimeError("boom")
        assert c.get_fundamentals(["AAPL"]) == {}


# ---------------------------------------------------------------------------
# get_all_watchlist_ids / get_watchlist_items_by_id
# ---------------------------------------------------------------------------

class TestWatchlistIds:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_get_all_watchlist_ids(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = {
            "results": [
                {"display_name": "Materials", "id": "wl1"},
                {"display_name": "", "id": "wl2"},  # skipped (no name)
            ]
        }
        result = c.get_all_watchlist_ids()
        assert result == {"materials": "wl1"}

    @patch("robinhood_sync.robinhood_client.rh")
    def test_get_watchlist_items_by_id(self, mock_rh):
        c = _client()
        mock_rh.helper.request_get.return_value = {
            "results": [{"symbol": "AAPL"}]
        }
        items = c.get_watchlist_items_by_id("wl1")
        assert items == [{"symbol": "AAPL"}]

    @patch("robinhood_sync.robinhood_client.rh")
    def test_get_watchlist_items_by_id_non_dict(self, mock_rh):
        c = _client()
        mock_rh.helper.request_get.return_value = None
        assert c.get_watchlist_items_by_id("wl1") == []


# ---------------------------------------------------------------------------
# get_earnings
# ---------------------------------------------------------------------------

class TestGetEarnings:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_returns_data(self, mock_rh):
        c = _client()
        mock_rh.stocks.get_earnings.return_value = [{"report": {}}]
        assert c.get_earnings("AAPL") == [{"report": {}}]

    @patch("robinhood_sync.robinhood_client.rh")
    def test_none_returns_empty(self, mock_rh):
        c = _client()
        mock_rh.stocks.get_earnings.return_value = None
        assert c.get_earnings("AAPL") == []


# ---------------------------------------------------------------------------
# get_stop_orders
# ---------------------------------------------------------------------------

class TestGetStopOrders:
    def _stop_order_dict(self, **overrides):
        d = {
            "id": "s1", "trigger": "stop", "state": "confirmed",
            "instrument": "http://inst/1", "stop_price": "90",
            "quantity": "10", "side": "sell", "type": "market",
            "price": None, "created_at": "2026-01-01T00:00:00Z",
        }
        d.update(overrides)
        return d

    @patch("robinhood_sync.robinhood_client.rh")
    def test_no_orders(self, mock_rh):
        c = _client()
        mock_rh.orders.get_all_stock_orders.return_value = []
        assert c.get_stop_orders() == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_parses_stop_order(self, mock_rh):
        c = _client()
        mock_rh.orders.get_all_stock_orders.return_value = [self._stop_order_dict()]
        with patch.object(c, "_get_symbol_from_instrument", return_value="AAPL"):
            orders = c.get_stop_orders()
        assert len(orders) == 1
        assert orders[0].symbol == "AAPL"
        assert orders[0].stop_price == Decimal("90")

    @patch("robinhood_sync.robinhood_client.rh")
    def test_skips_non_stop_trigger(self, mock_rh):
        c = _client()
        mock_rh.orders.get_all_stock_orders.return_value = [
            self._stop_order_dict(trigger="immediate")
        ]
        assert c.get_stop_orders() == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_skips_filled_state(self, mock_rh):
        c = _client()
        mock_rh.orders.get_all_stock_orders.return_value = [
            self._stop_order_dict(state="filled")
        ]
        assert c.get_stop_orders() == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_skips_missing_fields(self, mock_rh):
        c = _client()
        mock_rh.orders.get_all_stock_orders.return_value = [
            self._stop_order_dict(id=None),
            self._stop_order_dict(instrument=""),
            self._stop_order_dict(stop_price=None),
            None,
        ]
        with patch.object(c, "_get_symbol_from_instrument", return_value="AAPL"):
            assert c.get_stop_orders() == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_with_limit_price(self, mock_rh):
        c = _client()
        mock_rh.orders.get_all_stock_orders.return_value = [
            self._stop_order_dict(type="limit", price="88")
        ]
        with patch.object(c, "_get_symbol_from_instrument", return_value="AAPL"):
            orders = c.get_stop_orders()
        assert orders[0].limit_price == Decimal("88")

    @patch("robinhood_sync.robinhood_client.rh")
    def test_error_raises(self, mock_rh):
        c = _client()
        mock_rh.orders.get_all_stock_orders.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            c.get_stop_orders()


# ---------------------------------------------------------------------------
# get_all_watchlist_symbols
# ---------------------------------------------------------------------------

class TestGetAllWatchlistSymbols:
    @patch("robinhood_sync.robinhood_client.rh")
    def test_no_watchlists(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = []
        assert c.get_all_watchlist_symbols() == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_aggregates_unique_symbols(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = [
            {"display_name": "A"}, {"display_name": "B"},
        ]
        from robinhood_sync.robinhood_client import WatchlistStock

        def fake_get_watchlist(name):
            now = datetime.now(timezone.utc)
            if name == "A":
                return [WatchlistStock("AAPL", "Apple", "", now)]
            return [WatchlistStock("AAPL", "Apple", "", now),
                    WatchlistStock("MSFT", "Microsoft", "", now)]

        with patch.object(c, "get_watchlist", side_effect=fake_get_watchlist):
            symbols = c.get_all_watchlist_symbols()
        assert symbols == ["AAPL", "MSFT"]

    @patch("robinhood_sync.robinhood_client.rh")
    def test_per_watchlist_error_skipped(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.return_value = [{"display_name": "A"}]
        with patch.object(c, "get_watchlist", side_effect=RuntimeError("boom")):
            assert c.get_all_watchlist_symbols() == []

    @patch("robinhood_sync.robinhood_client.rh")
    def test_top_level_error_raises(self, mock_rh):
        c = _client()
        mock_rh.account.get_all_watchlists.side_effect = RuntimeError("fatal")
        with pytest.raises(RuntimeError):
            c.get_all_watchlist_symbols()
