---
sidebar_label: set_variable_activity_translator
title: wkmigrate.translators.activity_translators.set_variable_activity_translator
---

This module defines a translator for translating Set Variable activities.

Translators in this module normalize Set Variable activity payloads into internal
representations. The variable name and value are pulled from the Set Variable activity.

If the Set Variable activity references a complex expression (e.g. '@activity("activity_name").output.value'),
the expression is parsed into an equivalent Python expression.

#### translate\_set\_variable\_activity

```python
def translate_set_variable_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None
) -> tuple[SetVariableActivity | UnsupportedValue, TranslationContext]
```

Translates an ADF Set Variable activity into a ``SetVariableActivity`` object.

The activity's ``value`` field may be a static string or an ADF expression object. Supported
expressions are translated into Python code snippets. Any expression that cannot be translated
produces an ``UnsupportedValue``.

**Arguments**:

- `activity` - SetVariable activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
- `context` - Translation context.  When ``None`` a fresh default context is created.
  

**Returns**:

  A tuple with the translated result and the updated context.

