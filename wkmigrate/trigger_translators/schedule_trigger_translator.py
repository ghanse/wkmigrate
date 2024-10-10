""" This module defines methods for translating Databricks schedule triggers from data pipelines."""
from wkmigrate.trigger_translators.parsers import parse_cron_expression
from wkmigrate.utils import translate


mapping = {
    'quartz_cron_expression': {'key': 'recurrence', 'parser': parse_cron_expression},
    'timezone_id': {'key': 'time_zone', 'parser': lambda x: 'UTC'}
}


def translate_schedule_trigger(trigger_definition: dict) -> dict:
    """ Translates a schedule trigger definition in Data Factory's object model to a Databricks cron
        schedule definition in the Databricks SDK object model.
        :parameter trigger_definition: Schedule trigger definition as a ``dict``
        :return: Databricks cron schedule definition as a ``dict``
    """
    # Get the properties:
    if 'properties' not in trigger_definition:
        raise ValueError('No value for "properties" with trigger')
    properties = trigger_definition.get('properties')
    # Get the recurrence:
    if 'recurrence' not in properties:
        raise ValueError('No value for "recurrence" with schedule trigger')
    # Translate the properties:
    return translate(properties, mapping)
