---
sidebar_label: not_translatable
title: wkmigrate.not_translatable
---

Helpers for tracking non-translatable pipeline properties.

### not\_translatable\_context

```python
@contextmanager
def not_translatable_context(activity_name: str | None,
                             activity_type: str | None)
```

Captures activity metadata for warnings raised inside the context.

**Arguments**:

- `activity_name` - Logical name of the activity being translated.
- `activity_type` - Activity type string emitted by ADF.

## NotTranslatableWarning Objects

```python
class NotTranslatableWarning(UserWarning)
```

Custom warning for properties that cannot be translated.

### \_\_init\_\_

```python
def __init__(property_name: str, message: str) -> None
```

Initializes the warning and attaches contextual metadata.

**Arguments**:

- `property_name` - Pipeline property that could not be translated.
- `message` - Human-readable warning message.

