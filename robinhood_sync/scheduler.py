"""
Market hours scheduler for Robinhood sync.

Handles scheduling syncs during trading hours (including extended hours):
- Sunday: 6:00 PM - 11:59 PM ET (futures/pre-positioning)
- Monday-Friday: 4:00 AM - 8:00 PM ET (pre-market through after-hours)
- Saturday: Closed
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
        sunday_open_hour: int = 18,  # 6 PM ET
        poll_interval_minutes: int = 10,
    ):
        """
        Initialize the market scheduler.

        Args:
            market_open_hour: Hour when extended trading starts Mon-Fri (ET), default 4 AM.
            market_close_hour: Hour when extended trading ends Mon-Fri (ET), default 8 PM.
            sunday_open_hour: Hour when Sunday evening session starts (ET), default 6 PM.
            poll_interval_minutes: How often to poll during market hours.
        """
        self.market_open = time(hour=market_open_hour, minute=0)
        self.market_close = time(hour=market_close_hour, minute=0)
        self.sunday_open = time(hour=sunday_open_hour, minute=0)
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

        Schedule:
        - Sunday 6 PM - midnight ET (futures/pre-positioning)
        - Monday-Friday 4 AM - 8 PM ET (pre-market through after-hours)
        - Saturday: Closed

        Args:
            dt: Datetime to check, defaults to current ET time.

        Returns:
            True if during active trading hours.
        """
        if dt is None:
            dt = self.get_current_et_time()

        current_time = dt.time()
        day_of_week = dt.weekday()  # Monday = 0, Sunday = 6

        # Saturday (5) - always closed
        if day_of_week == 5:
            return False

        # Sunday (6) - open from 6 PM onwards
        if day_of_week == 6:
            return current_time >= self.sunday_open

        # Monday-Friday (0-4) - open 4 AM to 8 PM
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

        current_time = dt.time()
        day_of_week = dt.weekday()  # Monday = 0, Sunday = 6

        # If currently in market hours, return now (we're open)
        if self.is_market_hours(dt):
            return dt

        # Saturday - next open is Sunday 6 PM
        if day_of_week == 5:
            next_day = dt + timedelta(days=1)
            return next_day.replace(
                hour=self.sunday_open.hour,
                minute=0,
                second=0,
                microsecond=0,
            )

        # Sunday before 6 PM - next open is Sunday 6 PM (today)
        if day_of_week == 6 and current_time < self.sunday_open:
            return dt.replace(
                hour=self.sunday_open.hour,
                minute=0,
                second=0,
                microsecond=0,
            )

        # Monday-Friday before market open - next open is today at 4 AM
        if day_of_week < 5 and current_time < self.market_open:
            return dt.replace(
                hour=self.market_open.hour,
                minute=0,
                second=0,
                microsecond=0,
            )

        # Monday-Thursday after market close - next open is tomorrow 4 AM
        if day_of_week < 4 and current_time >= self.market_close:
            next_day = dt + timedelta(days=1)
            return next_day.replace(
                hour=self.market_open.hour,
                minute=0,
                second=0,
                microsecond=0,
            )

        # Friday after market close - next open is Sunday 6 PM
        if day_of_week == 4 and current_time >= self.market_close:
            sunday = dt + timedelta(days=2)
            return sunday.replace(
                hour=self.sunday_open.hour,
                minute=0,
                second=0,
                microsecond=0,
            )

        # Default: next weekday at market open
        next_day = dt + timedelta(days=1)
        while next_day.weekday() >= 5:  # Skip to Monday if weekend
            next_day = next_day + timedelta(days=1)
        return next_day.replace(
            hour=self.market_open.hour,
            minute=0,
            second=0,
            microsecond=0,
        )

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
        logger.info(f"  Schedule: Sun {self.sunday_open.strftime('%H:%M')}-24:00, Mon-Fri {status['market_open']}-{status['market_close']} ET")
        logger.info(f"  Currently open: {status['is_market_hours']}")

        if not status["is_market_hours"]:
            logger.info(f"  Next open: {status.get('next_market_open', 'N/A')}")
            logger.info(f"  Time until open: {status.get('time_until_open', 'N/A')}")
