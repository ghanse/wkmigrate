---
sidebar_label: definition_store_builder
title: wkmigrate.definition_stores.definition_store_builder
---

This module defines methods for building ``DefinitionStore`` objects.

### build\_definition\_store

```python
def build_definition_store(definition_store_type: str,
                           options: dict | None = None) -> DefinitionStore
```

Builds a ``DefinitionStore`` instance for the provided type.

**Arguments**:

- `definition_store_type` - Unique key for the registered definition store.
- `options` - Initialization keyword arguments for the definition store constructor.
  

**Returns**:

  Instantiated ``DefinitionStore`` object of the specified type.
  

**Raises**:

- `ValueError` - If the definition store type is unknown.
- `ValueError` - If required options are missing for the specified definition store type.

