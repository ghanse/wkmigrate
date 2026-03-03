---
sidebar_label: utils
title: wkmigrate.utils
---

This module defines shared utilities for translating data pipelines.

Utilities in this module cover common translation patterns such as mapping
dictionaries with parser specifications, normalizing expressions, and enriching
metadata (e.g. appending system tags).

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

#### parse\_mapping

```python
def parse_mapping(
        mapping: dict[str, Any] | None,
        parser: Callable[[Any], Any] | None = None) -> dict[str, Any]
```

Parses dictionary values into strings.

**Arguments**:

- `mapping` - Dictionary of key-value pairs
- `parser` - Method to apply to each mapping value
  

**Returns**:

  Mapping with parsed values

#### append\_system\_tags

```python
def append_system_tags(tags: dict | None) -> dict
```

Appends the ``CREATED_BY_WKMIGRATE`` system tag to a set of job tags.

**Arguments**:

- `tags` - Existing job tags.
  

**Returns**:

- `dict` - Updated tag dictionary.

#### parse\_activity\_timeout\_string

```python
def parse_activity_timeout_string(timeout_string: str,
                                  prefix: str = "") -> int
```

Parses a timeout string in the format ``d.hh:mm:ss`` into seconds.

**Arguments**:

- `timeout_string` - Timeout string from the activity policy.
- `prefix` - Prefix to add to the timeout string to align with the format 'd.hh:mm:ss'.
  

**Returns**:

  Total seconds represented by the timeout.

#### parse\_authentication

```python
def parse_authentication(
        secret_key: str, authentication: dict | None
) -> Authentication | UnsupportedValue | None
```

Parses an ADF authentication configuration into an ``Authentication`` object.

**Arguments**:

- `secret_key` - Secret scope key for the password.
- `authentication` - Authentication dictionary from the ADF activity, or ``None``.
  

**Returns**:

  Parsed ``Authentication`` or ``None`` when no auth is configured.

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

#### get\_data\_source\_definition

```python
def get_data_source_definition(
    dataset_definitions: list[dict] | UnsupportedValue
) -> Dataset | UnsupportedValue
```

Parses the first dataset definition from an activity into a ``Dataset`` object.

Validates that the definition contains the required ``properties`` and ``type``
fields before delegating to the dataset translator.

**Arguments**:

- `dataset_definitions` - Raw dataset definitions list from the ADF activity, or an
  ``UnsupportedValue`` propagated from an earlier validation step.
  

**Returns**:

  Parsed ``Dataset`` or ``UnsupportedValue`` when parsing fails.

#### get\_data\_source\_properties

```python
def get_data_source_properties(
    data_source_definition: dict | UnsupportedValue
) -> dict | UnsupportedValue
```

Parses data-source properties from an ADF activity source or sink block.

Validates that the definition contains a ``type`` field and delegates to
``parse_format_options`` to produce a format-specific options dictionary.

**Arguments**:

- `data_source_definition` - Source or sink definition from the ADF activity, or an
  ``UnsupportedValue`` propagated from an earlier validation step.
  

**Returns**:

  Data-source properties as a ``dict`` or ``UnsupportedValue`` when parsing fails.

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

**Arguments**:

- `result` - Activity or UnsupportedValue as an internal representation
- `base_kwargs` - Activity keyword-arguments
  

**Returns**:

  A placeholder DatabricksNotebookActivity for any UnsupportedValue; Otherwise the input Activity

