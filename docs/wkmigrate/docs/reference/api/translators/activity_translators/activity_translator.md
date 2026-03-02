---
sidebar_label: activity_translator
title: wkmigrate.translators.activity_translators.activity_translator
---

This module defines an activity translator from ADF payloads to internal IR.

The activity translator routes each ADF activity to its corresponding translator, stitches in
shared metadata (policy, dependencies, cluster specs), and flattens nested control-flow
constructs. It also captures non-translatable warnings so that callers receive structured
diagnostics with the translated activities.

Translation state is captured in a ``TranslationContext`` that is threaded through function calls
and returned alongside results.  No mutable state is shared between functions — each state transition
produces a new context, making the data flow fully explicit.

#### default\_context

```python
def default_context() -> TranslationContext
```

Creates a ``TranslationContext`` initialised with the default type-translator registry.

**Returns**:

  Fresh ``TranslationContext`` with an empty activity cache and the default registry.

#### translate\_activities\_with\_context

```python
def translate_activities_with_context(
    activities: list[dict] | None,
    context: TranslationContext | None = None
) -> tuple[list[Activity] | None, TranslationContext]
```

Translates a collection of ADF activities in dependency-first order, returning the
final translation context alongside the results.

Activities with no upstream dependencies are visited first, followed by their
dependents.  Each translated activity is stored in the returned context so that
callers can inspect the final cache.

**Arguments**:

- `activities` - List of raw ADF activity definitions, or ``None``.
- `context` - Optional translation context.  When ``None`` a fresh context with the
  default type-translator registry is used.
  

**Returns**:

  Tuple of ``(translated_activities, final_context)``.  The activity list is
  ``None`` when no input was provided.

#### translate\_activities

```python
def translate_activities(
        activities: list[dict] | None) -> list[Activity] | None
```

Translates a collection of ADF activities into a flattened list of ``Activity`` objects.

This is a convenience wrapper around ``translate_activities_with_context`` that
discards the final context.

**Arguments**:

- `activities` - List of activity definitions to translate.
  

**Returns**:

  Flattened list of translated activities as a ``list[Activity]`` or ``None`` when
  no input was provided.

#### translate\_activity

```python
def translate_activity(activity: dict,
                       is_conditional_task: bool = False) -> Activity
```

Translates a single ADF activity into an ``Activity`` object.

**Arguments**:

- `activity` - Activity definition emitted by ADF.
- `is_conditional_task` - Whether the task is a conditional task.
  

**Returns**:

  Translated ``Activity`` object.

#### visit\_activity

```python
def visit_activity(
        activity: dict, is_conditional_task: bool,
        context: TranslationContext) -> tuple[Activity, TranslationContext]
```

Translates a single ADF activity, returning the result and an updated context.

If the activity has already been translated the cached result is returned with the
context unchanged.

**Arguments**:

- `activity` - Activity definition emitted by ADF.
- `is_conditional_task` - Whether the task lives inside a conditional branch.
- `context` - Current translation context.
  

**Returns**:

  Tuple of ``(translated_activity, updated_context)``.

