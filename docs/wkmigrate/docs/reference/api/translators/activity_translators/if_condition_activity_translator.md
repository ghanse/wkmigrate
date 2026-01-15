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
def translate_if_condition_activity(
        activity: dict,
        base_kwargs: dict) -> IfConditionActivity | UnsupportedValue
```

Translates an ADF IfCondition activity into a ``IfConditionActivity`` object. IfCondition activities are translated into If-Else tasks in Databricks Lakeflow Jobs.

This method returns an ``UnsuportedValue`` if the activity cannot be translated. This can be due to:
* Missing conditional expression
* Unparseable conditional expression

**Arguments**:

- `activity` - IfCondition activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``IfConditionActivity`` representation of the IfCondition task and an optional list of nested activities (for child activities).

