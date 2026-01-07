"""This module defines methods for building ``DefinitionStore`` objects."""

from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores import types


def build_definition_store(definition_store_type: str, options: dict | None = None) -> DefinitionStore:
    """
    Builds a ``DefinitionStore`` instance for the provided type.

    Args:
        definition_store_type: Unique key for the registered definition store.
        options: Initialization keyword arguments for the definition store constructor.

    Returns:
        Instantiated ``DefinitionStore`` object of the specified type.

    Raises:
        ValueError: If the definition store type is unknown.
        ValueError: If required options are missing for the specified definition store type.
    """
    getter = types.get(definition_store_type, None)
    if getter is None:
        raise ValueError(f"No definition store registered with type {definition_store_type}")
    if options is None:
        raise ValueError(f"Options must be provided for definition store type {definition_store_type}")
    return getter(**options)
