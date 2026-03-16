"""This module defines methods for translating Databricks schedule triggers from data pipelines."""

from wkmigrate.translators.trigger_translators.parsers import parse_cron_expression

_DEFAULT_TIMEZONE = "UTC"


def translate_schedule_trigger(trigger_definition: dict) -> dict:
    """
    Translates a schedule trigger definition in Data Factory's object model to the Databricks SDK cron schedule format.

    Args:
        trigger_definition: Schedule trigger definition as a ``dict``.

    Returns:
        Databricks cron schedule definition as a ``dict``.

    Raises:
        ValueError: If the trigger definition is missing required properties.
    """
    properties = trigger_definition.get("properties")
    if properties is None:
        raise ValueError('No value for "properties" with trigger')
    if "recurrence" not in properties:
        raise ValueError('No value for "recurrence" with schedule trigger')
    return {
        "quartz_cron_expression": parse_cron_expression(properties.get("recurrence")),
        "timezone_id": _DEFAULT_TIMEZONE,
    }
