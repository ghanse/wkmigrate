---
sidebar_label: parsers
title: wkmigrate.pipeline_translators.parsers
---

This module defines methods for parsing pipeline objects to the Databricks SDK&#x27;s object model.

### parse\_parameter\_value

```python
def parse_parameter_value(parameter_value: Any) -> Any
```

Parses a parameter value and normalizes it to JSON-friendly formatting.

**Arguments**:

- `parameter_value` - Raw parameter value.
  

**Returns**:

  Parsed parameter value with inferred data type.

