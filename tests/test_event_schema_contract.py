"""Contract tests: validate the Kafka payloads robinhood-sync actually emits
against the platform's single source of truth, ``trading-event-schemas``.

These are TEST-ONLY. They build REAL event payloads by driving the production
code paths — the same ``RobinhoodClient`` parsers (``_parse_order``,
``get_current_positions``, ``get_account_balance``), the same ``Trade`` /
``Position`` ``to_dict`` serialization, the same ``sync.py`` watchlist
enrichment, and the same ``TradeEventProducer`` envelope builders — then assert
the emitted envelope conforms to its JSON Schema.

Nothing here hand-writes the event dict: each payload is captured from the
mocked ``KafkaProducer.send(value=...)`` call exactly as it would be put on the
wire, JSON round-tripped through the production ``value_serializer`` to prove it
is actually serializable, and finally handed to ``trading_event_schemas.validate``.

If a real payload fails to validate, that is a genuine contract mismatch (a bug
in this producer or an inaccuracy in the schema) and the test SHOULD fail loudly
rather than be weakened.
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from trading_event_schemas import validate

from robinhood_sync.kafka_producer import TradeEventProducer
from robinhood_sync.robinhood_client import RobinhoodClient


# The exact serializer the production KafkaProducer is configured with
# (see TradeEventProducer.connect: value_serializer=lambda v: json.dumps(v)...).
def _on_the_wire(event: dict) -> dict:
    """Round-trip an event through the production JSON serializer.

    This proves the captured envelope is actually serializable (catching, e.g.,
    a stray dataclass or Decimal that ``json.dumps`` would reject) and yields
    the plain dict that would land on Kafka and reach a consumer.
    """
    return json.loads(json.dumps(event))


@pytest.fixture
def producer():
    """A TradeEventProducer whose underlying KafkaProducer is mocked.

    Captures the envelope passed to ``send(value=...)`` without touching a
    broker, while still running the real publish_* envelope-builder code.
    """
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


def _captured_event(producer) -> dict:
    """Return the value dict from the most recent producer.send(...) call."""
    _, kwargs = producer._producer.send.call_args
    return kwargs["value"]


# ---------------------------------------------------------------------------
# Realistic raw Robinhood API structures (shaped like the real API responses
# the service parses in production).
# ---------------------------------------------------------------------------


def _raw_order() -> dict:
    """A raw Robinhood stock-order dict as returned by get_all_stock_orders."""
    return {
        "id": "a1b2c3d4-0000-1111-2222-333344445555",
        "instrument": "https://api.robinhood.com/instruments/inst-pltr/",
        "side": "buy",
        "cumulative_quantity": "25.00000000",
        "average_price": "42.50000000",
        "executed_notional": {"amount": "1062.50", "currency_code": "USD"},
        "fees": "0.02",
        "state": "filled",
        "executions": [
            {"price": "42.50", "quantity": "25.00000000",
             "timestamp": "2026-03-10T14:30:00.123456Z"}
        ],
        "last_transaction_at": "2026-03-10T14:30:01Z",
        "created_at": "2026-03-10T14:29:00.000000Z",
    }


def _raw_holdings() -> dict:
    """A raw build_holdings() dict (keyed by symbol) as returned by Robinhood."""
    return {
        "PLTR": {
            "quantity": "25.00000000",
            "average_buy_price": "42.50",
            "equity": "1125.00",
            "percent_change": "5.88",
            "equity_change": "62.50",
        },
        "AAPL": {
            "quantity": "3.00000000",
            "average_buy_price": "175.50",
            "equity": "540.00",
            "percent_change": "2.56",
            "equity_change": "13.50",
        },
    }


def _raw_account_profile() -> dict:
    return {"buying_power": "234.50", "cash": "234.50"}


def _raw_portfolio_profile() -> dict:
    return {"equity": "1899.50"}


def _raw_watchlist_items() -> list[dict]:
    """Raw midlands /lists/items/ dicts as get_watchlist_items_by_id returns,
    shaped the way sync.py consumes them (dicts carrying 'symbol' and 'name')."""
    return [
        {"symbol": "NVDA", "name": "NVIDIA Corporation",
         "instrument_url": "https://api.robinhood.com/instruments/inst-nvda/"},
        {"symbol": "AMD", "name": "Advanced Micro Devices, Inc.",
         "instrument_url": "https://api.robinhood.com/instruments/inst-amd/"},
    ]


@pytest.fixture
def client():
    """A RobinhoodClient marked logged-in, with API calls left to be patched."""
    c = RobinhoodClient(username="u", password="p")
    c._logged_in = True
    return c


# ---------------------------------------------------------------------------
# trade_event (TRADE_DETECTED)
# ---------------------------------------------------------------------------


def test_trade_event_real_payload_validates(producer, client):
    """Drive a raw order through _parse_order -> Trade.to_dict -> publish_trade
    and validate the emitted TRADE_DETECTED envelope against trade_event."""
    # Real parser path; only the instrument->symbol lookup is stubbed.
    with patch.object(client, "_get_symbol_from_instrument", return_value="PLTR"):
        trade = client._parse_order(_raw_order())
    assert trade is not None and trade.symbol == "PLTR"

    assert producer.publish_trade(trade) is True
    event = _on_the_wire(_captured_event(producer))

    # Will raise jsonschema.ValidationError on any contract mismatch.
    validate("trade_event", event)

    # Sanity: these are the load-bearing discriminators / required fields.
    assert event["event_type"] == "TRADE_DETECTED"
    assert event["source"] == "robinhood"
    assert event["data"]["order_id"] == "a1b2c3d4-0000-1111-2222-333344445555"
    assert event["data"]["side"] == "buy"


# ---------------------------------------------------------------------------
# positions_event (POSITIONS_SNAPSHOT)
# ---------------------------------------------------------------------------


def test_positions_event_real_payload_validates(producer, client):
    """Drive raw holdings + profiles through get_current_positions /
    get_account_balance -> Position.to_dict -> publish_positions and validate
    the emitted POSITIONS_SNAPSHOT envelope against positions_event."""
    with patch("robin_stocks.robinhood.account.build_holdings",
               return_value=_raw_holdings()), \
         patch("robin_stocks.robinhood.profiles.load_account_profile",
               return_value=_raw_account_profile()), \
         patch("robin_stocks.robinhood.profiles.load_portfolio_profile",
               return_value=_raw_portfolio_profile()):
        positions = client.get_current_positions()
        balance = client.get_account_balance()

    assert len(positions) == 2

    assert producer.publish_positions(positions, balance) is True
    event = _on_the_wire(_captured_event(producer))

    validate("positions_event", event)

    assert event["event_type"] == "POSITIONS_SNAPSHOT"
    assert event["source"] == "robinhood"
    assert event["data"]["total_equity"] == "1899.50"
    assert {p["symbol"] for p in event["data"]["positions"]} == {"PLTR", "AAPL"}


# ---------------------------------------------------------------------------
# watchlist_event_updated (WATCHLIST_UPDATED)
# ---------------------------------------------------------------------------


def test_watchlist_updated_real_payload_validates(producer):
    """Replicate sync.py's watchlist path: raw midlands dicts, enriched with
    fundamentals, passed to publish_watchlist_update; validate the emitted
    WATCHLIST_UPDATED envelope against watchlist_event_updated."""
    stocks = _raw_watchlist_items()

    # Mirror sync.py's fundamentals enrichment over the raw dicts.
    fundamentals = {
        "NVDA": {"sector": "Technology", "industry": "Semiconductors"},
        "AMD": {"sector": "Technology", "industry": "Semiconductors"},
    }
    for stock in stocks:
        fund = fundamentals.get(stock["symbol"], {})
        if fund.get("sector"):
            stock["sector"] = fund["sector"]
        if fund.get("industry"):
            stock["industry"] = fund["industry"]

    all_symbols = sorted(s["symbol"] for s in stocks)

    assert producer.publish_watchlist_update(
        added_symbols=["NVDA", "AMD"],
        removed_symbols=["INTC"],
        all_symbols=all_symbols,
        stocks=stocks,
    ) is True
    event = _on_the_wire(_captured_event(producer))

    validate("watchlist_event_updated", event)

    assert event["event_type"] == "WATCHLIST_UPDATED"
    assert event["source"] == "robinhood"
    assert event["data"]["total_count"] == 2
    assert {s["symbol"] for s in event["data"]["stocks"]} == {"NVDA", "AMD"}


# ---------------------------------------------------------------------------
# watchlist_event_added (WATCHLIST_SYMBOL_ADDED)
# ---------------------------------------------------------------------------


def test_watchlist_added_real_payload_validates(producer):
    """Drive publish_symbol_added (the single-add path sync.py calls per added
    symbol) and validate the emitted WATCHLIST_SYMBOL_ADDED envelope against
    watchlist_event_added."""
    assert producer.publish_symbol_added(
        "NVDA", "NVIDIA Corporation",
        sector="Technology", industry="Semiconductors",
    ) is True
    event = _on_the_wire(_captured_event(producer))

    validate("watchlist_event_added", event)

    assert event["event_type"] == "WATCHLIST_SYMBOL_ADDED"
    assert event["source"] == "robinhood"
    assert event["data"]["symbol"] == "NVDA"
    assert event["data"]["name"] == "NVIDIA Corporation"


def test_watchlist_added_minimal_payload_validates(producer):
    """The minimal single-add path (no sector/industry) must also validate:
    name falls back to the symbol, sector/industry are omitted."""
    assert producer.publish_symbol_added("TSLA") is True
    event = _on_the_wire(_captured_event(producer))

    validate("watchlist_event_added", event)

    assert event["data"]["symbol"] == "TSLA"
    assert event["data"]["name"] == "TSLA"
    assert "sector" not in event["data"]
