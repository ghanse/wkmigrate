---
sidebar_label: parameter_translator
title: wkmigrate.pipeline_translators.parameter_translator
---

This module defines methods for translating Databricks parameter values from data pipelines.

### translate\_parameters

```python
def translate_parameters(parameters: dict | None) -> list[dict] | None
```

Translates parameter definitions in the Data Factory object model to the Databricks SDK object model.

**Arguments**:

- `parameters` - Nested dictionary of parameter definitions.
  

**Returns**:

  List of translated parameter definitions as ``list[dict]`` objects, or ``None`` if no parameters are provided.

### translate\_parameter

```python
def translate_parameter(parameter_name: str, parameter: dict) -> dict
```

Translates a single parameter definition from the Data Factory object model to the Databricks SDK object model.

**Arguments**:

- `parameter_name` - Parameter name.
- `parameter` - Parameter definition as a ``dict``.
  

**Returns**:

  Translated parameter definition as a ``dict``.

