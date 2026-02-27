---
sidebar_label: utils
title: wkmigrate.preparers.utils
---

Shared helpers for workflow preparers.

#### get\_base\_task

```python
def get_base_task(activity: Activity) -> dict[str, Any]
```

Returns the fields common to every task.

**Arguments**:

- `activity` - Activity instance emitted by the translator.
  

**Returns**:

  Dictionary containing the common task fields.

