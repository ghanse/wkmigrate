""" This module defines methods for translating activities from data pipelines."""
from typing import Optional
from wkmigrate.activity_translators import type_mapping, type_translators
from wkmigrate.activity_translators.parsers import parse_dependencies, parse_policy
from wkmigrate.linked_service_translators.databricks_linked_service_translator import translate_cluster_spec
from wkmigrate.utils import identity, translate


mapping = {
    'type': {
        'key': 'type',
        'parser': identity
    },
    'task_key': {
        'key': 'name',
        'parser': lambda x: x if x else 'TASK_NAME_NOT_PROVIDED'
    },
    'description': {
        'key': 'description',
        'parser': identity
    },
    'timeout_seconds': {
        'key': 'policy',
        'parser': lambda x: parse_policy(x).get('timeout_seconds')
    },
    'max_retries': {
        'key': 'policy',
        'parser': lambda x: parse_policy(x).get('max_retries')
    },
    'min_retry_interval_millis': {
        'key': 'policy',
        'parser': lambda x: parse_policy(x).get('min_retry_interval_millis')
    },
    'depends_on': {
        'key': 'depends_on',
        'parser': parse_dependencies
    },
    'new_cluster': {
        'key': 'linked_service_definition',
        'parser': translate_cluster_spec
    }
}


def translate_activities(activities: Optional[list[dict]]) -> Optional[list[dict]]:
    """ Translates a set of data factory pipeline activities to a common object model.
        :parameter activities: Source pipeline activities
        :return: Target pipeline activities
    """
    if activities is None:
        return None
    translated_activities = []
    for activity in activities:
        translated_activity = translate_activity(activity)
        if isinstance(translated_activity, tuple):
            translated_activities.append(translated_activity[0])
            translated_activities.extend(translate_activities(translated_activity[1]))
            continue
        translated_activities.append(translated_activity)
    return translated_activities


def translate_activity(activity: dict) -> dict | tuple[dict, list[dict]]:
    """ Translates a data pipeline activity to a common object model.
        :parameter activity: Dictionary definition of the source pipeline activity
        :return: Dictionary definition of the target workflows activity"""
    # Translate the activity properties:
    translated_activity = translate(activity, mapping)
    # Parse the type properties for the task type:
    parsed_properties = parse_activity_properties(activity)
    # Check if any downstream activities were created:
    if isinstance(parsed_properties, tuple):
        return (translated_activity | parsed_properties[0]), parsed_properties[1]
    return translated_activity | parsed_properties


def parse_activity_properties(activity: dict) -> dict | tuple[dict, list[dict]]:
    """ Parses a data factory pipeline activity policy to a common object model.
        :parameter activity: Pipeline activity in the data factory object model as ``dict``
        :return: Workflow task in the Databricks SDK object model as ``dict``
    """
    translated_activity = {}
    # Translate the activity:
    translated_task = type_translators[activity.get('type')](activity)
    # Check if any downstream activities were created:
    if isinstance(translated_task, tuple):
        translated_activity[type_mapping[activity.get('type')]] = translated_task[0]
        return translated_activity, translated_task[1]
    translated_activity[type_mapping[activity.get('type')]] = translated_task
    return translated_activity
