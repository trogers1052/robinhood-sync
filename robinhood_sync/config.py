"""
Configuration management for Robinhood Sync Service.

``Settings`` subclasses the shared :class:`trading_commons.config.BaseServiceSettings`,
which provides the Kafka / Redis / Telegram blocks, the ``kafka_broker_list`` and
``redis_url`` helpers, and Docker-secrets support. Robinhood-specific fields and
the original Docker-secrets scope (only the robinhood_* credentials) are preserved
here so behavior is identical to the previous standalone Settings.
"""

from typing import ClassVar, Optional

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from trading_commons.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    """Application settings loaded from Docker secrets or environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Robinhood credentials
    robinhood_username: str = Field(..., description="Robinhood account username/email")
    robinhood_password: str = Field(..., description="Robinhood account password")
    robinhood_totp_secret: Optional[str] = Field(
        None, description="TOTP secret for 2FA (optional, for automated login)"
    )
    robinhood_device_token: Optional[str] = Field(
        None,
        description=(
            "Pinned Robinhood device identity. Robinhood challenges *new* devices, "
            "so keeping this stable is what stops a lost session from turning into "
            "a device approval the Pi cannot answer. Generated and persisted "
            "automatically if unset."
        ),
    )

    # Docker secrets scope — preserve the original behavior: ONLY the robinhood
    # credentials are sourced from /run/secrets (not redis/telegram), so a
    # mounted credential overrides the environment exactly as before.
    SECRET_FIELDS: ClassVar[tuple[str, ...]] = (
        "robinhood_username",
        "robinhood_password",
        "robinhood_totp_secret",
        "robinhood_device_token",
    )

    # Kafka configuration (kafka_brokers inherited; default localhost:19092)
    kafka_topic: str = Field("trading.orders", description="Kafka topic for trade events")
    kafka_positions_topic: str = Field(
        "trading.positions", description="Kafka topic for position snapshots"
    )
    kafka_watchlist_topic: str = Field(
        "trading.watchlist", description="Kafka topic for watchlist events"
    )

    # Redis configuration (host/port/password/db inherited from the base)
    redis_synced_orders_key: str = Field(
        "robinhood:synced_orders", description="Redis key for synced order IDs set"
    )
    redis_session_key: str = Field(
        "robinhood:session",
        description="Redis key holding the persisted OAuth session + device token",
    )

    # Session persistence / refresh policy.
    #
    # The session is stored in Redis (survives container recreation and volume
    # wipes, and is writable from the Mac by prime_session) with a local file
    # mirror as the fallback. Access tokens are refreshed at half-life so the
    # password login — the only path that can trigger a device challenge — is
    # effectively never reached.
    session_redis_enabled: bool = Field(
        True, description="Persist the session to Redis in addition to the file mirror"
    )
    robinhood_session_file: str = Field(
        "~/.tokens/robinhood_session.json",
        description="Local mirror of the persisted session document",
    )
    session_expires_in: int = Field(
        86400, description="Access token lifetime to request from Robinhood (seconds)"
    )
    session_refresh_ratio: float = Field(
        0.5,
        description="Refresh once this fraction of the access token's life has elapsed",
    )
    session_refresh_margin_sec: int = Field(
        900, description="Always refresh when fewer than this many seconds remain"
    )

    # Watchlist
    watchlist_names: str = Field("Materials", description="Comma-separated Robinhood watchlist names to sync")

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

    # Telegram alerts (telegram_bot_token / telegram_chat_id inherited from the
    # base) — used to notify a human when the login retry loop halts on an
    # unrecoverable auth failure.

    @property
    def watchlist_name_list(self) -> list[str]:
        """Return watchlist names as a list."""
        return [n.strip() for n in self.watchlist_names.split(",") if n.strip()]


def get_settings() -> Settings:
    """Load and return settings."""
    return Settings()
