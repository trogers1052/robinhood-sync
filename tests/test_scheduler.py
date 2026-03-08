"""Tests for MarketScheduler — pure datetime logic, no external dependencies."""

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from robinhood_sync.scheduler import MarketScheduler, ET


@pytest.fixture
def scheduler():
    return MarketScheduler(
        market_open_hour=4,
        market_close_hour=20,
        sunday_open_hour=18,
        poll_interval_minutes=10,
    )


def _et(year, month, day, hour, minute=0):
    """Create an ET datetime for testing."""
    return datetime(year, month, day, hour, minute, tzinfo=ET)


# ---------------------------------------------------------------------------
# is_weekday
# ---------------------------------------------------------------------------


class TestIsWeekday:
    def test_monday(self, scheduler):
        # 2026-02-16 is Monday
        assert scheduler.is_weekday(_et(2026, 2, 16, 10))

    def test_friday(self, scheduler):
        # 2026-02-20 is Friday
        assert scheduler.is_weekday(_et(2026, 2, 20, 10))

    def test_saturday(self, scheduler):
        # 2026-02-21 is Saturday
        assert not scheduler.is_weekday(_et(2026, 2, 21, 10))

    def test_sunday(self, scheduler):
        # 2026-02-22 is Sunday
        assert not scheduler.is_weekday(_et(2026, 2, 22, 10))


# ---------------------------------------------------------------------------
# is_market_hours — weekday
# ---------------------------------------------------------------------------


class TestIsMarketHoursWeekday:
    def test_during_market_hours(self, scheduler):
        # Wednesday 10 AM ET — should be open
        assert scheduler.is_market_hours(_et(2026, 2, 18, 10))

    def test_at_market_open(self, scheduler):
        # 4:00 AM — exactly at open
        assert scheduler.is_market_hours(_et(2026, 2, 18, 4, 0))

    def test_before_market_open(self, scheduler):
        # 3:59 AM — one minute before open
        assert not scheduler.is_market_hours(_et(2026, 2, 18, 3, 59))

    def test_at_market_close(self, scheduler):
        # 8:00 PM — exactly at close (close is exclusive)
        assert not scheduler.is_market_hours(_et(2026, 2, 18, 20, 0))

    def test_just_before_close(self, scheduler):
        # 7:59 PM
        assert scheduler.is_market_hours(_et(2026, 2, 18, 19, 59))

    def test_after_market_close(self, scheduler):
        # 9:00 PM
        assert not scheduler.is_market_hours(_et(2026, 2, 18, 21, 0))

    def test_midnight_weekday(self, scheduler):
        assert not scheduler.is_market_hours(_et(2026, 2, 18, 0, 0))

    def test_premarket_3am(self, scheduler):
        assert not scheduler.is_market_hours(_et(2026, 2, 18, 3, 0))


# ---------------------------------------------------------------------------
# is_market_hours — Saturday (always closed)
# ---------------------------------------------------------------------------


class TestIsMarketHoursSaturday:
    def test_saturday_morning(self, scheduler):
        assert not scheduler.is_market_hours(_et(2026, 2, 21, 10, 0))

    def test_saturday_evening(self, scheduler):
        assert not scheduler.is_market_hours(_et(2026, 2, 21, 19, 0))

    def test_saturday_midnight(self, scheduler):
        assert not scheduler.is_market_hours(_et(2026, 2, 21, 0, 0))


# ---------------------------------------------------------------------------
# is_market_hours — Sunday
# ---------------------------------------------------------------------------


class TestIsMarketHoursSunday:
    def test_sunday_before_open(self, scheduler):
        # 2:00 PM on Sunday — before 6 PM
        assert not scheduler.is_market_hours(_et(2026, 2, 22, 14, 0))

    def test_sunday_at_open(self, scheduler):
        # 6:00 PM on Sunday — should be open
        assert scheduler.is_market_hours(_et(2026, 2, 22, 18, 0))

    def test_sunday_after_open(self, scheduler):
        # 9:00 PM on Sunday — should be open
        assert scheduler.is_market_hours(_et(2026, 2, 22, 21, 0))

    def test_sunday_just_before_open(self, scheduler):
        # 5:59 PM on Sunday
        assert not scheduler.is_market_hours(_et(2026, 2, 22, 17, 59))

    def test_sunday_11pm(self, scheduler):
        assert scheduler.is_market_hours(_et(2026, 2, 22, 23, 0))


# ---------------------------------------------------------------------------
# get_next_market_open
# ---------------------------------------------------------------------------


class TestGetNextMarketOpen:
    def test_during_market_hours_returns_now(self, scheduler):
        now = _et(2026, 2, 18, 10, 0)  # Wed 10 AM
        result = scheduler.get_next_market_open(now)
        assert result == now

    def test_saturday_returns_sunday_6pm(self, scheduler):
        sat = _et(2026, 2, 21, 12, 0)
        result = scheduler.get_next_market_open(sat)
        assert result.weekday() == 6  # Sunday
        assert result.hour == 18

    def test_sunday_before_6pm_returns_today_6pm(self, scheduler):
        sun = _et(2026, 2, 22, 10, 0)
        result = scheduler.get_next_market_open(sun)
        assert result.day == 22  # Same day
        assert result.hour == 18

    def test_sunday_after_6pm_returns_now(self, scheduler):
        sun = _et(2026, 2, 22, 19, 0)
        result = scheduler.get_next_market_open(sun)
        assert result == sun  # Already in market hours

    def test_weekday_before_4am_returns_today_4am(self, scheduler):
        early = _et(2026, 2, 18, 2, 0)  # Wed 2 AM
        result = scheduler.get_next_market_open(early)
        assert result.day == 18  # Same day
        assert result.hour == 4

    def test_monday_through_thursday_after_close(self, scheduler):
        # Wednesday 9 PM → Thursday 4 AM
        late = _et(2026, 2, 18, 21, 0)
        result = scheduler.get_next_market_open(late)
        assert result.day == 19  # Thursday
        assert result.hour == 4

    def test_friday_after_close_returns_sunday_6pm(self, scheduler):
        fri = _et(2026, 2, 20, 21, 0)  # Friday 9 PM
        result = scheduler.get_next_market_open(fri)
        assert result.weekday() == 6  # Sunday
        assert result.hour == 18


# ---------------------------------------------------------------------------
# get_sleep_duration
# ---------------------------------------------------------------------------


class TestGetSleepDuration:
    def test_during_market_hours_returns_poll_interval(self, scheduler):
        """During market hours, sleep = poll interval."""
        with pytest.MonkeyPatch.context() as m:
            m.setattr(scheduler, "get_current_et_time", lambda: _et(2026, 2, 18, 10))
            duration = scheduler.get_sleep_duration()
            assert duration == timedelta(minutes=10)

    def test_outside_market_hours_sleeps_until_open(self, scheduler):
        """Outside market hours, sleep > poll interval."""
        with pytest.MonkeyPatch.context() as m:
            # Saturday noon — next open is Sunday 6 PM (~30 hours)
            m.setattr(scheduler, "get_current_et_time", lambda: _et(2026, 2, 21, 12))
            duration = scheduler.get_sleep_duration()
            # Should be about 30 hours + 1 min buffer
            assert duration.total_seconds() > 3600 * 29

    def test_sleep_includes_buffer(self, scheduler):
        """Sleep includes 1-minute buffer past market open."""
        with pytest.MonkeyPatch.context() as m:
            # Wednesday 3:59 AM — 1 min before open → sleep ~1 min + 1 min buffer
            m.setattr(scheduler, "get_current_et_time", lambda: _et(2026, 2, 18, 3, 59))
            duration = scheduler.get_sleep_duration()
            # Open at 4:00 AM + 1 min buffer = ~2 min total
            assert 60 < duration.total_seconds() < 180


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------


class TestGetStatus:
    def test_status_during_market_hours(self, scheduler):
        with pytest.MonkeyPatch.context() as m:
            m.setattr(scheduler, "get_current_et_time", lambda: _et(2026, 2, 18, 10))
            status = scheduler.get_status()
            assert status["is_market_hours"] is True
            assert status["is_weekday"] is True
            assert "next_market_open" not in status

    def test_status_outside_market_hours(self, scheduler):
        with pytest.MonkeyPatch.context() as m:
            m.setattr(scheduler, "get_current_et_time", lambda: _et(2026, 2, 21, 12))
            status = scheduler.get_status()
            assert status["is_market_hours"] is False
            assert "next_market_open" in status
            assert "time_until_open" in status

    def test_status_has_day_of_week(self, scheduler):
        with pytest.MonkeyPatch.context() as m:
            m.setattr(scheduler, "get_current_et_time", lambda: _et(2026, 2, 18, 10))
            status = scheduler.get_status()
            assert status["day_of_week"] == "Wednesday"


# ---------------------------------------------------------------------------
# Custom schedule parameters
# ---------------------------------------------------------------------------


class TestCustomSchedule:
    def test_custom_market_open(self):
        s = MarketScheduler(market_open_hour=9, market_close_hour=16)
        assert s.market_open == time(9, 0)
        assert s.market_close == time(16, 0)

    def test_custom_hours_affects_is_market_hours(self):
        s = MarketScheduler(market_open_hour=9, market_close_hour=16)
        # 8 AM should be closed (open at 9)
        assert not s.is_market_hours(_et(2026, 2, 18, 8, 0))
        # 9 AM should be open
        assert s.is_market_hours(_et(2026, 2, 18, 9, 0))
        # 4 PM should be closed (close at 16)
        assert not s.is_market_hours(_et(2026, 2, 18, 16, 0))

    def test_custom_sunday_open(self):
        s = MarketScheduler(sunday_open_hour=20)
        # 7 PM Sunday — before custom 8 PM open
        assert not s.is_market_hours(_et(2026, 2, 22, 19, 0))
        # 8 PM Sunday — at custom open
        assert s.is_market_hours(_et(2026, 2, 22, 20, 0))

    def test_custom_poll_interval(self):
        s = MarketScheduler(poll_interval_minutes=5)
        assert s.poll_interval == timedelta(minutes=5)
