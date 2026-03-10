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
- `registry` - Read-only mapping of activity type strings to their translator callables.
- `variable_cache` - Read-only mapping of variable names to the task keys of the task that sets the variable value.

#### with\_activity

```python
def with_activity(name: str, activity: Activity) -> TranslationContext
```

Returns a new context with an activity added to the cache.

**Arguments**:

- `name` - Activity name used as the cache key.
- `activity` - Translated ``Activity`` to store.
  

**Returns**:

  New ``TranslationContext`` containing the updated activity cache.

#### get\_activity

```python
def get_activity(activity_name: str) -> Activity | None
```

Looks up a previously translated activity by name.

**Arguments**:

- `activity_name` - Activity name.
  

**Returns**:

  Cached ``Activity`` or ``None`` if the name has not been visited.

#### with\_variable

```python
def with_variable(variable_name: str, task_key: str) -> TranslationContext
```

Returns a new context with a variable added to the cache.

**Arguments**:

- `variable_name` - Variable name used as the cache key.
- `task_key` - Task key for the task which set the variable value.
  

**Returns**:

  New ``TranslationContext`` containing the updated variable cache.

#### get\_variable\_task\_key

```python
def get_variable_task_key(variable_name: str) -> str | None
```

Looks up the task key which set a variable.

**Arguments**:

- `variable_name` - Variable name.
  

**Returns**:

  Cached task key of the task that set the variable value.

