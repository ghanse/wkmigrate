---
sidebar_label: copy_activity_translator
title: wkmigrate.activity_translators.copy_activity_translator
---

This module defines methods for translating Copy activities.

### translate\_copy\_activity

```python
def translate_copy_activity(activity: dict, base_kwargs: dict) -> CopyActivity
```

Translates an ADF Copy activity into a ``CopyActivity`` object.

**Arguments**:

- `activity` - Copy activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``CopyActivity`` representation of the Copy task.

