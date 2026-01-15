"""This module provides a factory function for constructing `DefinitionStore` instances.

The builder resolves a store key (for example ``\"factory_definition_store\"``)
into a concrete class registered in ``wkmigrate.definition_stores.types`` and
instantiates it with user-provided options. Use this when wiring stores from
configuration files or CLIs to avoid coupling to specific implementations.

Example:
    ```python
    from wkmigrate.definition_stores.definition_store_builder import build_definition_store

    options = {
        \"tenant_id\": \"...\", \"client_id\": \"...\", \"client_secret\": \"...\",
        \"subscription_id\": \"...\", \"resource_group_name\": \"...\", \"factory_name\": \"...\"
    }
    store = build_definition_store(\"factory_definition_store\", options)
    ```
"""

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
