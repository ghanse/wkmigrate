---
sidebar_label: if_condition_activity_translator
title: wkmigrate.translators.activity_translators.if_condition_activity_translator
---

This module defines a translator for translating If Condition activities.

Translators in this module normalize If Condition activity payloads into internal representations.
Each translator must validate required fields, parse the activity's condition expression, and emit
``UnsupportedValue`` objects for any unparsable inputs.

#### translate\_if\_condition\_activity

```python
@translates_activity("IfCondition")
def translate_if_condition_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None
) -> tuple[TranslationResult, TranslationContext]
```

Translates an ADF IfCondition activity into a ``IfConditionActivity`` object.

The context is threaded through each child activity translation so that the
activity cache accumulates across branches.

This method returns an ``UnsupportedValue`` as the first element if the activity
cannot be translated due to a missing or unparseable conditional expression.

**Arguments**:

- `activity` - IfCondition activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
- `context` - Translation context.  When ``None`` a fresh default context is created.
  

**Returns**:

  A tuple with the translated result and the updated context.

