---
sidebar_label: activity_translator
title: wkmigrate.translators.activity_translators.activity_translator
---

This module defines an activity translator from ADF payloads to internal IR.

The activity translator routes each ADF activity to its corresponding translator, stitches in 
shared metadata (policy, dependencies, cluster specs), and flattens nested control-flow 
constructs. It also captures non-translatable warnings so that callers receive structured 
diagnostics with the translated activities.

#### translate\_activities

```python
def translate_activities(
        activities: list[dict] | None) -> list[Activity] | None
```

Translates a collection of ADF activities into a flattened list of ``Activity`` objects.

**Arguments**:

- `activities` - List of activity definitions to translate.
  

**Returns**:

  Flattened list of translated activities as a ``list[Activity]`` or ``None`` when no input was provided.

#### translate\_activity

```python
def translate_activity(activity: dict) -> Activity
```

Translates a single ADF activity into an ``Activity`` object.

**Arguments**:

- `activity` - Activity definition emitted by ADF.
  

**Returns**:

  Translated activity and an optional list of nested activities (for If/ForEach activities).

