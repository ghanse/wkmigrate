---
sidebar_label: utils
title: wkmigrate.utils
---

This module defines shared utilities for translating data pipelines.

### identity

```python
def identity(item: Any) -> Any
```

Returns the provided value unchanged.

### translate

```python
def translate(items: dict | None, mapping: dict) -> dict | None
```

Maps dictionary values using a translation specification.

**Arguments**:

- `items` - Source dictionary.
- `mapping` - Translation specification; Each key defines a ``key`` to look up and a ``parser`` callable.
  

**Returns**:

  Translated dictionary as a ``dict`` or ``None`` when no input is provided.

### append\_system\_tags

```python
def append_system_tags(tags: dict | None) -> dict
```

Appends the ``CREATED_BY_WKMIGRATE`` system tag to a set of job tags.

**Arguments**:

- `tags` - Existing job tags.
  

**Returns**:

- `dict` - Updated tag dictionary.

### parse\_expression

```python
def parse_expression(expression: str) -> str
```

Parses a variable or parameter expression to a Workflows-compatible parameter value.

**Arguments**:

- `expression` - Variable or parameter expression as a ``str``.
  

**Returns**:

  Workflows-compatible parameter value as a ``str``.

