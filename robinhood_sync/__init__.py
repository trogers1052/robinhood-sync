"""
Robinhood Sync Service

Syncs trades from Robinhood to Kafka for processing by other services.
"""

from .config import Settings, get_settings
from .robinhood_client import RobinhoodClient, Trade
from .kafka_producer import TradeEventProducer
from .sync import TradeSyncService

__version__ = "0.1.0"

__all__ = [
    "Settings",
    "get_settings",
    "RobinhoodClient",
    "Trade",
    "TradeEventProducer",
    "TradeSyncService",
]
