import pytest

from wkmigrate.enums.interval_type import IntervalType
from wkmigrate.translators.trigger_translators.parsers import parse_cron_expression
from wkmigrate.translators.trigger_translators.schedule_trigger_translator import (
    translate_schedule_trigger,
)


class TestTriggerTranslator:
    """Unit tests for trigger translator methods."""

    @pytest.mark.parametrize(
        "trigger_definition, expected_result",
        [
            (
                {
                    "properties": {
                        "recurrence": {
                            "frequency": "Day",
                            "interval": 1,
                            "schedule": {"hours": [9], "minutes": [0]},
                        },
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 0 9 */1 * ?", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {
                            "frequency": "Day",
                            "interval": 2,
                            "schedule": {"hours": [9], "minutes": [23]},
                        },
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 23 9 */2 * ?", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {"frequency": "Hour", "interval": 1},
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 0 */1 * * ?", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {"frequency": "Hour", "interval": 5},
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 0 */5 * * ?", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {"frequency": "Week", "interval": 1},
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 0 0 ? * 1", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {"frequency": "Week", "interval": 5},
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 0 0 ? * 1", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {
                            "frequency": "Week",
                            "interval": 1,
                            "schedule": {
                                "week_days": ["Sunday", "Wednesday"],
                                "hours": [9],
                                "minutes": [15],
                            },
                        },
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 15 9 ? * 1,4", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {"frequency": "Month", "interval": 1},
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 0 0 0 * ?", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {
                            "frequency": "Month",
                            "interval": 1,
                            "schedule": {
                                "days": [3, 12],
                                "hours": [9],
                                "minutes": [15],
                            },
                        },
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 15 9 3,12 * ?", "timezone_id": "UTC"},
            ),
            (
                {
                    "properties": {
                        "recurrence": {
                            "frequency": "Month",
                            "interval": 5,
                            "schedule": {
                                "days": [3, 12],
                                "hours": [9],
                                "minutes": [15],
                            },
                        },
                        "time_zone": "Eastern Standard Time",
                    }
                },
                {"quartz_cron_expression": "0 15 9 3,12 * ?", "timezone_id": "UTC"},
            ),
        ],
    )
    def test_translate_schedule_trigger_parses_result(self, trigger_definition, expected_result):
        result = translate_schedule_trigger(trigger_definition)
        assert result == expected_result

    @pytest.mark.parametrize(
        "trigger_definition, expected_error_message",
        [
            ({}, 'No value for "properties" with trigger'),
            ({"properties": {}}, 'No value for "recurrence" with schedule trigger'),
        ],
    )
    def test_translate_schedule_trigger_excepts(self, trigger_definition, expected_error_message):
        with pytest.raises(ValueError, match=expected_error_message):
            translate_schedule_trigger(trigger_definition)

    @pytest.mark.parametrize(
        "extra_property, extra_value",
        [
            ("extra_property", "should be ignored"),
            ("another_extra", 123),
            ("yet_another", {"nested": "value"}),
        ],
    )
    def test_translate_schedule_trigger_ignores(self, extra_property, extra_value):
        input_trigger = {
            "properties": {
                "recurrence": {
                    "frequency": "Day",
                    "interval": 1,
                    "schedule": {"hours": [9], "minutes": [0]},
                },
                "time_zone": "UTC",
                extra_property: extra_value,
            }
        }
        result = translate_schedule_trigger(input_trigger)
        assert extra_property not in result

    @pytest.mark.parametrize(
        "recurrence, expected_result",
        [
            (None, None),
            ({"frequency": IntervalType.HOUR, "interval": 2}, "0 0 */2 * * ?"),
            (
                {
                    "frequency": IntervalType.DAY,
                    "interval": 1,
                    "schedule": {"minutes": [30], "hours": [9]},
                },
                "0 30 9 */1 * ?",
            ),
            (
                {
                    "frequency": IntervalType.WEEK,
                    "schedule": {
                        "minutes": [0],
                        "hours": [8],
                        "week_days": ["Monday", "Wednesday"],
                    },
                },
                "0 0 8 ? * 2,4",
            ),
            (
                {
                    "frequency": IntervalType.MONTH,
                    "schedule": {"minutes": [15], "hours": [10], "days": [1, 15]},
                },
                "0 15 10 1,15 * ?",
            ),
        ],
    )
    def test_parse_cron_expression(self, recurrence, expected_result):
        assert parse_cron_expression(recurrence) == expected_result

    @pytest.mark.parametrize(
        "recurrence",
        [
            {
                "frequency": IntervalType.WEEK,
                "interval": 2,
                "schedule": {"week_days": ["Monday"]},
            },
            {"frequency": IntervalType.MONTH, "interval": 3, "schedule": {"days": [1]}},
        ],
    )
    def test_parse_cron_expression_warnings(self, recurrence):
        with pytest.warns(UserWarning):
            parse_cron_expression(recurrence)
