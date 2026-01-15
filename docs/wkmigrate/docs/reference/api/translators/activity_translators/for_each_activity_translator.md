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
def translate_for_each_activity(
        activity: dict,
        base_kwargs: dict) -> ForEachActivity | UnsupportedValue
```

Translates an ADF ForEach activity into a ``ForEachActivity`` object. ForEach activities are translated into For Each tasks in Databricks Lakeflow Jobs.

This method returns an ``UnsupportedValue`` if the activity cannot be translated. This can be due to:
* Missing or invalid items expression
* Unparseable items expression

**Arguments**:

- `activity` - ForEach activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``ForEachActivity`` representation of the ForEach task.

