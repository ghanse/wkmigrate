"""This module defines shared utilities for translating data pipelines."""

from typing import Any


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
