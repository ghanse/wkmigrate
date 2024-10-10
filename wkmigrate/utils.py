""" This module defines shared utilities for translating data pipelines."""
from typing import Any, Optional


def identity(item: Any) -> Any:
    return item


def translate(items: Optional[dict], mapping: dict) -> Optional[dict]:
    if items is None:
        return None
    output = {}
    for key, value in mapping.items():
        source_key = mapping[key]['key']
        parser = mapping[key]['parser']
        value = parser(items.get(source_key))
        if value is not None:
            output[key] = value
    return output


def append_system_tags(tags: Optional[dict]) -> dict:
    """ Appends system tags for attributing clusters to the Tributary library.
        :parameter tags: Optional set of user-defined tags as a ``dict``
        :return: Set of tags with 'CREATED_BY_WKMIGRATE' appended.
    """
    if tags is None:
        return {'CREATED_BY_WKMIGRATE': ''}
    else:
        tags['CREATED_BY_WKMIGRATE'] = ''
        return tags


def parse_expression(expression: str) -> str:
    """ Parses a variable or parameter expression to a Workflows-compatible parameter value.
        :parameter expression: Variable or parameter expression as a ``str``
        :return: Workflows-compatible parameter value as a ``str``
    """
    # TODO: ADD DIFFERENT FUNCTIONS TO BE PARSED INTO {{}} OPERATORS
    return expression
