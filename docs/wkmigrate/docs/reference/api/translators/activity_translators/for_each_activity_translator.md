---
sidebar_label: for_each_activity_translator
title: wkmigrate.translators.activity_translators.for_each_activity_translator
---

This module defines a translator for translating For Each activities.

Translators in this module normalize For Each activity payloads into internal representations.
Each translator must validate required fields, parse the activity's items expression, and emit
``UnsupportedValue`` objects for any unparsable inputs.

#### translate\_for\_each\_activity

```python
@translates_activity("ForEach")
def translate_for_each_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None
) -> tuple[TranslationResult, TranslationContext]
```

Translates an ADF ForEach activity into a ``ForEachActivity`` object.

ForEach activities are translated into For Each tasks in Databricks Lakeflow Jobs.

This method returns an ``UnsupportedValue`` as the first element if the activity
cannot be translated due to missing items or inner activities.

**Arguments**:

- `activity` - ForEach activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
- `context` - Translation context.  When ``None`` a fresh default context is created.
  

**Returns**:

  A tuple with the translated result and the updated context.

