"""Tests for Settings configuration — properties, defaults, env var loading."""

import os

import pytest

from robinhood_sync.config import Settings


# ---------------------------------------------------------------------------
# kafka_broker_list property
# ---------------------------------------------------------------------------


class TestKafkaBrokerList:
    def test_single_broker(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            kafka_brokers="localhost:9092",
        )
        assert s.kafka_broker_list == ["localhost:9092"]

    def test_multiple_brokers(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            kafka_brokers="b1:9092,b2:9092,b3:9092",
        )
        assert s.kafka_broker_list == ["b1:9092", "b2:9092", "b3:9092"]

    def test_whitespace_stripped(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            kafka_brokers=" b1:9092 , b2:9092 ",
        )
        assert s.kafka_broker_list == ["b1:9092", "b2:9092"]


# ---------------------------------------------------------------------------
# redis_url property
# ---------------------------------------------------------------------------


class TestRedisUrl:
    def test_without_password(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
        )
        assert s.redis_url == "redis://localhost:6379/0"

    def test_with_password(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            redis_host="redis-host",
            redis_port=6380,
            redis_password="secret",
            redis_db=2,
        )
        assert s.redis_url == "redis://:secret@redis-host:6380/2"


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------


class TestDefaults:
    def test_kafka_defaults(self):
        s = Settings(robinhood_username="u", robinhood_password="p")
        assert s.kafka_brokers == "localhost:19092"
        assert s.kafka_topic == "trading.orders"
        assert s.kafka_positions_topic == "trading.positions"
        assert s.kafka_watchlist_topic == "trading.watchlist"

    def test_redis_defaults(self):
        s = Settings(robinhood_username="u", robinhood_password="p")
        assert s.redis_host == "localhost"
        assert s.redis_port == 6379
        assert s.redis_db == 0
        assert s.redis_password is None

    def test_sync_defaults(self):
        s = Settings(robinhood_username="u", robinhood_password="p")
        assert s.poll_interval_minutes == 10
        assert s.sync_history_days == 30

    def test_market_hours_defaults(self):
        s = Settings(robinhood_username="u", robinhood_password="p")
        assert s.market_open_hour == 4
        assert s.market_close_hour == 20

    def test_totp_default_none(self):
        s = Settings(robinhood_username="u", robinhood_password="p")
        assert s.robinhood_totp_secret is None

    def test_synced_orders_key_default(self):
        s = Settings(robinhood_username="u", robinhood_password="p")
        assert s.redis_synced_orders_key == "robinhood:synced_orders"


# ---------------------------------------------------------------------------
# Custom values
# ---------------------------------------------------------------------------


class TestWatchlistNameList:
    def test_single_name_from_watchlist_name(self):
        s = Settings(robinhood_username="u", robinhood_password="p")
        assert s.watchlist_name_list == ["Materials"]

    def test_multiple_names_from_watchlist_names(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            watchlist_names="Materials,Technology,Healthcare",
        )
        assert s.watchlist_name_list == ["Materials", "Technology", "Healthcare"]

    def test_whitespace_stripped(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            watchlist_names=" Materials , Tech ",
        )
        assert s.watchlist_name_list == ["Materials", "Tech"]


class TestCustomValues:
    def test_custom_poll_interval(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            poll_interval_minutes=5,
        )
        assert s.poll_interval_minutes == 5

    def test_custom_totp(self):
        s = Settings(
            robinhood_username="u",
            robinhood_password="p",
            robinhood_totp_secret="JBSWY3DPEHPK3PXP",
        )
        assert s.robinhood_totp_secret == "JBSWY3DPEHPK3PXP"
