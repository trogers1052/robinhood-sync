"""
Configuration management for Robinhood Sync Service.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Robinhood credentials
    robinhood_username: str = Field(..., description="Robinhood account username/email")
    robinhood_password: str = Field(..., description="Robinhood account password")
    robinhood_totp_secret: Optional[str] = Field(
        None, description="TOTP secret for 2FA (optional, for automated login)"
    )

    # Kafka configuration
    kafka_brokers: str = Field("localhost:19092", description="Kafka broker addresses")
    kafka_topic: str = Field("trading.orders", description="Kafka topic for trade events")
    kafka_positions_topic: str = Field(
        "trading.positions", description="Kafka topic for position snapshots"
    )
    kafka_watchlist_topic: str = Field(
        "trading.watchlist", description="Kafka topic for watchlist events"
    )

    # Redis configuration (for tracking synced orders)
    redis_host: str = Field("localhost", description="Redis host")
    redis_port: int = Field(6379, description="Redis port")
    redis_password: Optional[str] = Field(None, description="Redis password (optional)")
    redis_db: int = Field(0, description="Redis database number")
    redis_synced_orders_key: str = Field(
        "robinhood:synced_orders", description="Redis key for synced order IDs set"
    )

    # Sync configuration
    poll_interval_minutes: int = Field(
        10, description="How often to poll Robinhood during market hours (minutes)"
    )
    sync_history_days: int = Field(
        30, description="How many days of history to sync on first run"
    )

    # Market hours (Eastern Time)
    market_open_hour: int = Field(4, description="Market open hour (ET) - pre-market starts")
    market_close_hour: int = Field(20, description="Market close hour (ET) - after-hours ends")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @property
    def kafka_broker_list(self) -> list[str]:
        """Return Kafka brokers as a list."""
        return [b.strip() for b in self.kafka_brokers.split(",")]

    @property
    def redis_url(self) -> str:
        """Return Redis connection URL."""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


def get_settings() -> Settings:
    """Load and return settings."""
    return Settings()
