"""
Market hours scheduler for Robinhood sync.

Handles scheduling syncs only during NYSE trading hours (including extended hours):
- Pre-market: 4:00 AM - 9:30 AM ET
- Regular: 9:30 AM - 4:00 PM ET
- After-hours: 4:00 PM - 8:00 PM ET

Only runs Monday-Friday (market is closed on weekends).
"""

import logging
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# NYSE timezone
ET = ZoneInfo("America/New_York")


class MarketScheduler:
    """Determines when to sync based on market hours."""

    def __init__(
        self,
        market_open_hour: int = 4,
        market_close_hour: int = 20,
        poll_interval_minutes: int = 10,
    ):
        """
        Initialize the market scheduler.

        Args:
            market_open_hour: Hour when extended trading starts (ET), default 4 AM.
            market_close_hour: Hour when extended trading ends (ET), default 8 PM.
            poll_interval_minutes: How often to poll during market hours.
        """
        self.market_open = time(hour=market_open_hour, minute=0)
        self.market_close = time(hour=market_close_hour, minute=0)
        self.poll_interval = timedelta(minutes=poll_interval_minutes)

    def get_current_et_time(self) -> datetime:
        """Get the current time in Eastern Time."""
        return datetime.now(ET)

    def is_weekday(self, dt: Optional[datetime] = None) -> bool:
        """
        Check if the given time is a weekday (Mon-Fri).

        Args:
            dt: Datetime to check, defaults to current ET time.

        Returns:
            True if Monday-Friday, False if weekend.
        """
        if dt is None:
            dt = self.get_current_et_time()
        # Monday = 0, Sunday = 6
        return dt.weekday() < 5

    def is_market_hours(self, dt: Optional[datetime] = None) -> bool:
        """
        Check if the given time is during market hours (including extended).

        Args:
            dt: Datetime to check, defaults to current ET time.

        Returns:
            True if during market hours on a weekday.
        """
        if dt is None:
            dt = self.get_current_et_time()

        # Check if weekday first
        if not self.is_weekday(dt):
            return False

        # Check if within market hours
        current_time = dt.time()
        return self.market_open <= current_time < self.market_close

    def get_next_market_open(self, dt: Optional[datetime] = None) -> datetime:
        """
        Get the next market open time.

        Args:
            dt: Starting datetime, defaults to current ET time.

        Returns:
            Datetime of next market open.
        """
        if dt is None:
            dt = self.get_current_et_time()

        # Start from tomorrow if we're past market close today
        current_time = dt.time()
        if current_time >= self.market_close:
            dt = dt + timedelta(days=1)

        # Find next weekday
        while not self.is_weekday(dt):
            dt = dt + timedelta(days=1)

        # Set to market open time
        next_open = dt.replace(
            hour=self.market_open.hour,
            minute=self.market_open.minute,
            second=0,
            microsecond=0,
        )

        # If we're before market open today, use today
        if self.is_weekday(dt) and current_time < self.market_open:
            return dt.replace(
                hour=self.market_open.hour,
                minute=self.market_open.minute,
                second=0,
                microsecond=0,
            )

        return next_open

    def get_sleep_duration(self) -> timedelta:
        """
        Calculate how long to sleep before the next action.

        Returns:
            Timedelta for how long to sleep.
        """
        now = self.get_current_et_time()

        if self.is_market_hours(now):
            # During market hours, sleep for poll interval
            return self.poll_interval
        else:
            # Outside market hours, sleep until next market open
            next_open = self.get_next_market_open(now)
            sleep_duration = next_open - now

            # Add a small buffer (1 minute) to ensure we're past market open
            sleep_duration += timedelta(minutes=1)

            return sleep_duration

    def get_status(self) -> dict:
        """
        Get current market status information.

        Returns:
            Dict with market status details.
        """
        now = self.get_current_et_time()
        is_open = self.is_market_hours(now)

        status = {
            "current_time_et": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "day_of_week": now.strftime("%A"),
            "is_weekday": self.is_weekday(now),
            "is_market_hours": is_open,
            "market_open": self.market_open.strftime("%H:%M"),
            "market_close": self.market_close.strftime("%H:%M"),
        }

        if not is_open:
            next_open = self.get_next_market_open(now)
            status["next_market_open"] = next_open.strftime("%Y-%m-%d %H:%M:%S %Z")
            status["time_until_open"] = str(next_open - now)

        return status

    def log_status(self) -> None:
        """Log the current market status."""
        status = self.get_status()
        logger.info(f"Market Status: {status['current_time_et']} ({status['day_of_week']})")
        logger.info(f"  Is weekday: {status['is_weekday']}")
        logger.info(f"  Market hours: {status['market_open']} - {status['market_close']} ET")
        logger.info(f"  Currently open: {status['is_market_hours']}")

        if not status["is_market_hours"]:
            logger.info(f"  Next open: {status.get('next_market_open', 'N/A')}")
            logger.info(f"  Time until open: {status.get('time_until_open', 'N/A')}")
