---
sidebar_label: utils
title: wkmigrate.utils
---

This module defines shared utilities for translating data pipelines.

Utilities in this module cover common translation patterns such as mapping 
dictionaries with parser specifications, normalizing expressions, and enriching 
metadata (e.g. appending system tags).

#### identity

```python
def identity(item: Any) -> Any
```

Returns the provided value unchanged.

#### translate

```python
def translate(items: dict | None, mapping: dict) -> dict | None
```

Maps dictionary values using a translation specification.

**Arguments**:

- `items` - Source dictionary.
- `mapping` - Translation specification; Each key defines a ``key`` to look up and a ``parser`` callable.
  

**Returns**:

  Translated dictionary as a ``dict`` or ``None`` when no input is provided.

#### append\_system\_tags

```python
def append_system_tags(tags: dict | None) -> dict
```

Appends the ``CREATED_BY_WKMIGRATE`` system tag to a set of job tags.

**Arguments**:

- `tags` - Existing job tags.
  

**Returns**:

- `dict` - Updated tag dictionary.

#### parse\_expression

```python
def parse_expression(expression: str) -> str
```

Parses a variable or parameter expression to a Workflows-compatible parameter value.

**Arguments**:

- `expression` - Variable or parameter expression as a ``str``.
  

**Returns**:

  Workflows-compatible parameter value as a ``str``.

#### extract\_group

```python
def extract_group(input_string: str, regex: str) -> str | UnsupportedValue
```

Extracts a regex group from an input string.

**Arguments**:

- `input_string` - Input string to search.
- `regex` - Regex pattern to match.
  

**Returns**:

  Extracted group as a ``str``.

#### get\_value\_or\_unsupported

```python
def get_value_or_unsupported(items: dict, key: str) -> Any | UnsupportedValue
```

Gets a value from a dictionary or returns an ``UnsupportedValue`` object if the key is not found.

**Arguments**:

- `items` - Dictionary to search.
- `key` - Key to look up.
  

**Returns**:

  Value as a ``Any`` or ``UnsupportedValue`` object if the key is not found.

#### merge\_unsupported\_values

```python
def merge_unsupported_values(values: list[Any]) -> UnsupportedValue
```

Merges a list of unsupported values into a single ``UnsupportedValue`` object.

**Arguments**:

- `values` - List of translated values.
  

**Returns**:

  Single ``UnsupportedValue`` object.

#### get\_placeholder\_activity

```python
def get_placeholder_activity(base_kwargs: dict) -> DatabricksNotebookActivity
```

Creates a placeholder notebook task for unsupported activities.

**Arguments**:

- `base_kwargs` - Common task metadata.
  

**Returns**:

  Databricks ``NotebookActivity`` object as a placeholder task.

#### normalize\_translated\_result

```python
def normalize_translated_result(result: Activity | UnsupportedValue,
                                base_kwargs: dict) -> Activity
```

Normalizes translator results so callers always receive Activities.

Translators may return an ``UnsupportedValue`` to signal that an activity could not
be translated. In those cases, this helper converts the unsupported value into a
placeholder notebook activity so downstream components (such as the workflow
preparer) continue to operate on ``Activity`` instances only.

