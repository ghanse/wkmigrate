---
sidebar_label: if_condition_activity_translator
title: wkmigrate.activity_translators.if_condition_activity_translator
---

This module defines methods for translating If Condition activities.

### translate\_if\_condition\_activity

```python
def translate_if_condition_activity(
    activity: dict, base_kwargs: dict
) -> IfConditionActivity | tuple[IfConditionActivity, list[Activity]]
```

Translates an ADF IfCondition activity into a ``IfConditionActivity`` object.

**Arguments**:

- `activity` - IfCondition activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``IfConditionActivity`` representation of the IfCondition task and an optional list of nested activities (for child activities).

