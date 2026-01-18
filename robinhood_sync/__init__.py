"""
Robinhood Sync Service

Syncs trades from Robinhood to Kafka for processing by other services.
Runs continuously during market hours (Mon-Fri, 4am-8pm ET).
"""

from .config import Settings, get_settings
from .robinhood_client import RobinhoodClient, Trade
from .kafka_producer import TradeEventProducer
from .redis_client import SyncedOrdersTracker
from .scheduler import MarketScheduler
from .sync import TradeSyncService

__version__ = "0.2.0"

__all__ = [
    "Settings",
    "get_settings",
    "RobinhoodClient",
    "Trade",
    "TradeEventProducer",
    "SyncedOrdersTracker",
    "MarketScheduler",
    "TradeSyncService",
]
