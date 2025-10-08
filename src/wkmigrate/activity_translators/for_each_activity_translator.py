"""This module defines methods for translating For Each activities."""

from wkmigrate.activity_translators.parsers import (
    parse_for_each_items,
    parse_for_each_tasks,
)
from wkmigrate.utils import identity, translate


mapping = {
    "inputs": {"key": "items", "parser": parse_for_each_items},
    "concurrency": {"key": "batch_count", "parser": identity},
    "task": {"key": "activities", "parser": parse_for_each_tasks},
}


def translate_for_each_activity(activity: dict) -> dict:
    """Translates a For Each activity definition in Data Factory's object model to a Databricks for-each condition
    task in the Databricks SDK object model.
    :parameter activity: For Each activity definition as a ``dict``
    :return: Databricks for-each condition task properties as a ``dict``
    """
    translated = translate(activity, mapping)
    if translated is None:
        raise ValueError('Translation failed')
    return translated
