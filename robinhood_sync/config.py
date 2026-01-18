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

    # Database configuration (for tracking synced orders)
    db_host: str = Field("localhost", description="PostgreSQL host")
    db_port: int = Field(5432, description="PostgreSQL port")
    db_user: str = Field("trader", description="PostgreSQL user")
    db_password: str = Field("trader5", description="PostgreSQL password")
    db_name: str = Field("trading_platform", description="PostgreSQL database name")

    # Sync configuration
    poll_interval_seconds: int = Field(
        300, description="How often to poll Robinhood for new orders (seconds)"
    )
    sync_history_days: int = Field(
        30, description="How many days of history to sync on first run"
    )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @property
    def kafka_broker_list(self) -> list[str]:
        """Return Kafka brokers as a list."""
        return [b.strip() for b in self.kafka_brokers.split(",")]

    @property
    def database_url(self) -> str:
        """Return PostgreSQL connection URL."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


def get_settings() -> Settings:
    """Load and return settings."""
    return Settings()
