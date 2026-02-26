---
sidebar_label: translation_context
title: wkmigrate.models.ir.translation_context
---

This module defines the immutable translation context threaded through activity translation.

The ``TranslationContext`` captures all accumulated state produced during translation.  It is
a frozen dataclass so that every state transition is made explicit: functions receive a context,
and return a new one alongside their result.  This makes the data flow through the translation
pipeline fully transparent and side-effect free.

## TranslationContext Objects

```python
@dataclass(frozen=True, slots=True)
class TranslationContext()
```

Immutable snapshot of translation state threaded through each visitor call.

Every function that needs to read or extend the caches receives a
``TranslationContext`` and returns a new one — the original is never mutated.

**Attributes**:

- `activity_cache` - Read-only mapping of activity names to translated ``Activity`` objects.
- `registry` - Read-only mapping of ADF activity type strings to their translator callables.

#### with\_activity

```python
def with_activity(name: str, activity: Activity) -> TranslationContext
```

Returns a new context with an activity added to the cache.

**Arguments**:

- `name` - Logical activity name used as the cache key.
- `activity` - Translated ``Activity`` to store.
  

**Returns**:

  New ``TranslationContext`` containing the updated activity cache.

#### get\_activity

```python
def get_activity(name: str) -> Activity | None
```

Looks up a previously translated activity by name.

**Arguments**:

- `name` - Logical activity name.
  

**Returns**:

  Cached ``Activity`` or ``None`` if the name has not been visited.

