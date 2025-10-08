"""This module defines methods for building ``DefinitionStore`` objects."""

from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores import types


getters = types


def build_definition_store(definition_store_type: str, options: dict | None = None) -> DefinitionStore:
    """Gets a ``DefinitionStore`` object with the given options.
    :parameter definition_store_type: Definition store type
    :parameter options: A set of options for the specified definition store type
    :return: ``DefinitionStore``: A ``DefinitionStore`` of the specified type
    """
    getter = getters.get(definition_store_type, None)
    if getter is None:
        raise ValueError(f"No definition store registered with type {definition_store_type}")
    if options is None:
        raise ValueError(f"Options must be provided for definition store type {definition_store_type}")
    return getter(**options)
