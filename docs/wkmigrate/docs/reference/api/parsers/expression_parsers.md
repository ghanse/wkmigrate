---
sidebar_label: expression_parsers
title: wkmigrate.parsers.expression_parsers
---

#### parse\_variable\_value

```python
def parse_variable_value(
        value: str | dict,
        context: TranslationContext) -> str | UnsupportedValue
```

Parses an ADF variable value or expression into a Python code snippet. Unsupported dynamic expressions return
`UnsupportedValue`.

The following cases are supported:

* Static string values -> Python string literal (e.g. ``'hello'``).
* Expressions (e.g. ``{"value": "@...", "type": "Expression"}``) -> inner expression is extracted and parsed.
* Activity output references (e.g. ``@activity('X').output.Y``) -> ``dbutils.jobs.taskValues.get(taskKey='X', key='result')``.
* Pipeline system variables (e.g. ``@pipeline().Pipeline`` or ``@pipeline().RunId``) -> ``spark.conf`` or
``dbutils.jobs.getContext()`` lookups.
* Variables (e.g. ``@variables('X')``) -> ``dbutils.jobs.taskValues.get(taskKey='X', key='result')``.

**Arguments**:

- `value` - Variable value. Can be a plain string or an expression object with ``"type": "Expression"``.
- `context` - Translation context.
  

**Returns**:

  A Python expression string suitable for embedding in a generated notebook, or an `UnsupportedValue` when the
  expression cannot be translated.

