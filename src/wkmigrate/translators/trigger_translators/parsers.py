"""This module defines methods for parsing trigger objects to the Databricks SDK's object model."""

import warnings
from wkmigrate.enums.interval_type import IntervalType
from wkmigrate.not_translatable import NotTranslatableWarning


def parse_cron_expression(recurrence: dict | None) -> str | None:
    """
    Generates a quartz cron expression from a set of schedule trigger parameters.

    Args:
        recurrence: Recurrence object containing the frequency and schedule details.

    Returns:
        Cron expression as a ``str`` or ``None`` when no recurrence is provided.
    """
    if recurrence is None:
        return None
    interval_type = recurrence.get("frequency")
    num_intervals = recurrence.get("interval")
    if num_intervals is None:
        warnings.warn(
            NotTranslatableWarning("schedule.num_intervals", 'Setting empty "num_intervals" to "1" by default'),
            stacklevel=2,
        )
        num_intervals = 1
    schedule = recurrence.get("schedule")
    if interval_type == IntervalType.HOUR:
        return _get_hourly_cron_expression(num_intervals)
    if interval_type == IntervalType.DAY:
        return _get_daily_cron_expression(num_intervals, schedule or {})
    if interval_type == IntervalType.WEEK:
        if num_intervals > 1:
            warnings.warn(
                NotTranslatableWarning(
                    "schedule.num_intervals",
                    'Ignoring "num_intervals" > 1 for weekly triggers; Using weekly interval',
                ),
                stacklevel=2,
            )
        return _get_weekly_cron_expression(schedule or {})
    if interval_type == IntervalType.MONTH:
        if num_intervals > 1:
            warnings.warn(
                NotTranslatableWarning(
                    "schedule.num_intervals",
                    'Ignoring "num_intervals" > 1 for monthly triggers; Using monthly interval',
                ),
                stacklevel=2,
            )
        return _get_monthly_cron_expression(schedule or {})
    return None


def _get_hourly_cron_expression(num_intervals: int) -> str:
    """
    Builds a cron expression for an hourly schedule.

    Args:
        num_intervals: Hour interval between runs.

    Returns:
        Cron expression as a ``str``.
    """
    return f"0 0 */{num_intervals} * * ?"


def _get_daily_cron_expression(num_intervals: int, schedule: dict | None) -> str:
    """
    Builds a cron expression for a daily schedule.

    Args:
        num_intervals: Day interval between runs.
        schedule: Specific hours/minutes configuration.

    Returns:
        Cron expression as a ``str``.
    """
    if schedule is None:
        return f"0 0 0 */{num_intervals} * ?"
    minutes = ",".join([str(e) for e in schedule.get("minutes", [0])])
    hours = ",".join([str(e) for e in schedule.get("hours", [0])])
    return f"0 {minutes} {hours} */{num_intervals} * ?"


def _get_weekly_cron_expression(schedule: dict | None) -> str:
    """
    Builds a cron expression for a weekly schedule.

    Args:
        schedule: Dictionary containing minutes, hours, and week days.

    Returns:
        Cron expression as a ``str``.
    """
    if schedule is None:
        return "0 0 0 ? * 1"
    minutes = ",".join([str(e) for e in schedule.get("minutes", [0])])
    hours = ",".join([str(e) for e in schedule.get("hours", [0])])
    week_days = ",".join([_get_week_day(e) for e in schedule.get("week_days", ["Sunday"])])
    return f"0 {minutes} {hours} ? * {week_days}"


def _get_monthly_cron_expression(schedule: dict | None) -> str:
    """
    Builds a cron expression for a monthly schedule.

    Args:
        schedule: Dictionary containing minutes, hours, and days.

    Returns:
        Cron expression as a ``str``.
    """
    if schedule is None:
        return "0 0 0 0 * ?"
    minutes = ",".join([str(e) for e in schedule.get("minutes", [0])])
    hours = ",".join([str(e) for e in schedule.get("hours", [0])])
    days = ",".join([str(e) for e in schedule.get("days", [0])])
    return f"0 {minutes} {hours} {days} * ?"


def _get_week_day(week_day: str) -> str:
    """
    Converts a named weekday to the quartz cron numeric equivalent.

    Args:
        week_day: Weekday string (e.g., ``"Sunday"``).

    Returns:
        Cron weekday representation as a ``str``.

    Raises:
        ValueError: If the weekday string is not recognized.
    """
    if week_day == "Sunday":
        return "1"
    if week_day == "Monday":
        return "2"
    if week_day == "Tuesday":
        return "3"
    if week_day == "Wednesday":
        return "4"
    if week_day == "Thursday":
        return "5"
    if week_day == "Friday":
        return "6"
    if week_day == "Saturday":
        return "7"
    raise ValueError('Invalid value for parameter "week_day"')
