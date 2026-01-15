"""This module defines shared utilities for translating data pipelines.

Utilities in this module cover common translation patterns such as mapping
dictionaries with parser specifications, normalizing expressions, and enriching
metadata (e.g. appending system tags).
"""

import re
from typing import Any

from wkmigrate.models.ir.activities import Activity, DatabricksNotebookActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue


def identity(item: Any) -> Any:
    """Returns the provided value unchanged."""
    return item


def translate(items: dict | None, mapping: dict) -> dict | None:
    """
    Maps dictionary values using a translation specification.

    Args:
        items: Source dictionary.
        mapping: Translation specification; Each key defines a ``key`` to look up and a ``parser`` callable.

    Returns:
        Translated dictionary as a ``dict`` or ``None`` when no input is provided.
    """
    if items is None:
        return None
    output = {}
    for key, value in mapping.items():
        source_key = mapping[key]["key"]
        parser = mapping[key]["parser"]
        value = parser(items.get(source_key))
        if value is not None:
            output[key] = value
    return output


def append_system_tags(tags: dict | None) -> dict:
    """
    Appends the ``CREATED_BY_WKMIGRATE`` system tag to a set of job tags.

    Args:
        tags: Existing job tags.

    Returns:
        dict: Updated tag dictionary.
    """
    if tags is None:
        return {"CREATED_BY_WKMIGRATE": ""}

    tags["CREATED_BY_WKMIGRATE"] = ""
    return tags


def parse_expression(expression: str) -> str:
    """
    Parses a variable or parameter expression to a Workflows-compatible parameter value.

    Args:
        expression: Variable or parameter expression as a ``str``.

    Returns:
        Workflows-compatible parameter value as a ``str``.
    """
    # TODO: ADD DIFFERENT FUNCTIONS TO BE PARSED INTO {{}} OPERATORS
    return expression


def extract_group(input_string: str, regex: str) -> str | UnsupportedValue:
    """
    Extracts a regex group from an input string.

    Args:
        input_string: Input string to search.
        regex: Regex pattern to match.

    Returns:
        Extracted group as a ``str``.
    """
    match = re.search(pattern=regex, string=input_string)
    if match is None:
        return UnsupportedValue(
            value=input_string, message=f"No match for regex '{regex}' found in input string '{input_string}'"
        )
    return match.group(1)


def get_value_or_unsupported(items: dict, key: str) -> Any | UnsupportedValue:
    """
    Gets a value from a dictionary or returns an ``UnsupportedValue`` object if the key is not found.

    Args:
        items: Dictionary to search.
        key: Key to look up.

    Returns:
        Value as a ``Any`` or ``UnsupportedValue`` object if the key is not found.
    """
    value = items.get(key)
    if value is None:
        return UnsupportedValue(value=items, message=f"Missing value for key '{key}' in dictionary")
    return value


def merge_unsupported_values(values: list[Any]) -> UnsupportedValue:
    """
    Merges a list of unsupported values into a single ``UnsupportedValue`` object.

    Args:
        values: List of translated values.

    Returns:
        Single ``UnsupportedValue`` object.
    """
    unsupported = [value for value in values if isinstance(value, UnsupportedValue)]
    if unsupported:
        return UnsupportedValue(value=values, message=";".join([value.message for value in unsupported]))
    raise ValueError("No unsupported values in input list")


def get_placeholder_activity(base_kwargs: dict) -> DatabricksNotebookActivity:
    """
    Creates a placeholder notebook task for unsupported activities.

    Args:
        base_kwargs: Common task metadata.

    Returns:
        Databricks ``NotebookActivity`` object as a placeholder task.
    """
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path="/UNSUPPORTED_ADF_ACTIVITY",
    )


def normalize_translated_result(result: Activity | UnsupportedValue, base_kwargs: dict) -> Activity:
    """
    Normalizes translator results so callers always receive Activities.

    Translators may return an ``UnsupportedValue`` to signal that an activity could not
    be translated. In those cases, this helper converts the unsupported value into a
    placeholder notebook activity so downstream components (such as the workflow
    preparer) continue to operate on ``Activity`` instances only.
    """
    if isinstance(result, UnsupportedValue):
        return get_placeholder_activity(base_kwargs)

    return result
