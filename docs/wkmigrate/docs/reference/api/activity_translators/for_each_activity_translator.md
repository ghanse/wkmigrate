---
sidebar_label: for_each_activity_translator
title: wkmigrate.activity_translators.for_each_activity_translator
---

This module defines methods for translating For Each activities.

### translate\_for\_each\_activity

```python
def translate_for_each_activity(activity: dict,
                                base_kwargs: dict) -> ForEachActivity
```

Translates an ADF ForEach activity into a ``ForEachActivity`` object.

**Arguments**:

- `activity` - ForEach activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``ForEachActivity`` representation of the ForEach task.

