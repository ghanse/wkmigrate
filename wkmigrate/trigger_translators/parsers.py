""" This module defines methods for parsing trigger objects to the Databricks SDK's object model."""
import warnings
from typing import Optional
from wkmigrate.enums.interval_type import IntervalType


def parse_cron_expression(recurrence: Optional[dict]) -> Optional[str]:
    """ Generates a cron expression from a set of schedule trigger parameters.
        :parameter recurrence: Recurrence object as a ``IntervalType``
        :return: Cron expression for the specified parameters as a ``str``
    """
    if recurrence is None:
        return None
    interval_type = recurrence.get('frequency')
    num_intervals = recurrence.get('interval')
    if num_intervals is None:
        warnings.warn('Setting empty "num_intervals" to "1" by default', stacklevel=2)
        num_intervals = 1
    schedule = recurrence.get('schedule')
    if interval_type == IntervalType.HOUR:
        return _get_hourly_cron_expression(num_intervals)
    if interval_type == IntervalType.DAY:
        return _get_daily_cron_expression(num_intervals, schedule)
    if interval_type == IntervalType.WEEK:
        if num_intervals > 1:
            warnings.warn('Ignoring "num_intervals" > 1 for weekly triggers; Using weekly interval', stacklevel=2)
        return _get_weekly_cron_expression(schedule)
    if interval_type == IntervalType.MONTH:
        if num_intervals > 1:
            warnings.warn('Ignoring "num_intervals" > 1 for monthly triggers; Using monthly interval', stacklevel=2)
        return _get_monthly_cron_expression(schedule)


def _get_hourly_cron_expression(num_intervals: int) -> str:
    return f'0 0 */{num_intervals} * * ?'


def _get_daily_cron_expression(num_intervals: int, schedule: dict) -> str:
    if schedule is None:
        return f'0 0 0 */{num_intervals} * ?'
    minutes = ','.join([str(e) for e in schedule.get('minutes', [0])])
    hours = ','.join([str(e) for e in schedule.get('hours', [0])])
    return f'0 {minutes} {hours} */{num_intervals} * ?'


def _get_weekly_cron_expression(schedule: dict) -> str:
    if schedule is None:
        return '0 0 0 ? * 1'
    minutes = ','.join([str(e) for e in schedule.get('minutes', [0])])
    hours = ','.join([str(e) for e in schedule.get('hours', [0])])
    week_days = ','.join([_get_week_day(e) for e in schedule.get('week_days', ['Sunday'])])
    return f'0 {minutes} {hours} ? * {week_days}'


def _get_monthly_cron_expression(schedule: dict) -> str:
    if schedule is None:
        return '0 0 0 0 * ?'
    minutes = ','.join([str(e) for e in schedule.get('minutes', [0])])
    hours = ','.join([str(e) for e in schedule.get('hours', [0])])
    days = ','.join([str(e) for e in schedule.get('days', [0])])
    return f'0 {minutes} {hours} {days} * ?'


def _get_week_day(week_day: str) -> str:
    if week_day == 'Sunday':
        return '1'
    if week_day == 'Monday':
        return '2'
    if week_day == 'Tuesday':
        return '3'
    if week_day == 'Wednesday':
        return '4'
    if week_day == 'Thursday':
        return '5'
    if week_day == 'Friday':
        return '6'
    if week_day == 'Saturday':
        return '7'
    raise ValueError('Invalid value for parameter "week_day"')
