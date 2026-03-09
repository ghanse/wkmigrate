---
sidebar_label: expression_parsers
title: wkmigrate.parsers.expression_parsers
---

#### parse\_variable\_value

```python
def parse_variable_value(value: str | dict) -> str | UnsupportedValue
```

Parses an ADF variable value or expression into a Python code snippet. Unsupported dynamic expressions return
`UnsupportedValue`.

The following cases are supported:

* Static string (no leading ``@``) → Python string literal (e.g. ``'hello'``).
* ADF expression object ``{"value": "@...", "type": "Expression"}`` → inner expression is extracted and parsed.
* ``@activity('X').output.Y`` → ``dbutils.jobs.taskValues.get(taskKey='X', key='result')``.
* ``@pipeline().Pipeline`` / ``@pipeline().RunId`` / other supported system variables → ``spark.conf`` or
``dbutils.jobs.getContext()`` lookups.

**Arguments**:

- `value` - ADF variable value. Can be a plain string or an expression object with ``"type": "Expression"``.
  

**Returns**:

  A Python expression string suitable for embedding in a generated notebook, or an `UnsupportedValue` when the
  expression cannot be translated.

