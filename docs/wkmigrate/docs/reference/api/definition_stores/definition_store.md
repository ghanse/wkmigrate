---
sidebar_label: definition_store
title: wkmigrate.definition_stores.definition_store
---

This module defines the abstract ``DefinitionStore`` class.

## DefinitionStore Objects

```python
class DefinitionStore(ABC)
```

Abstract source or sink for pipeline definitions.

### load

```python
@abstractmethod
def load(pipeline_name: str) -> dict
```

Loads a pipeline definition.

**Arguments**:

- `pipeline_name` - Pipeline identifier as a ``str``.
  

**Returns**:

  Dictionary representation of the pipeline as a ``dict``.

### dump

```python
@abstractmethod
def dump(pipeline_definition: dict) -> int | None
```

Persists a pipeline definition.

**Arguments**:

- `pipeline_definition` - Pipeline definition emitted by the translators as a ``dict``.
  

**Returns**:

  Optional identifier for the stored workflow.

