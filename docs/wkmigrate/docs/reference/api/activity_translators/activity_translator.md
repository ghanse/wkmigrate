---
sidebar_label: activity_translator
title: wkmigrate.activity_translators.activity_translator
---

This module defines methods for translating activities from data pipelines.

### translate\_activities

```python
def translate_activities(
        activities: list[dict] | None) -> list[Activity] | None
```

Translates a collection of ADF activities into a list of ``Activity`` objects.

**Arguments**:

- `activities` - List of activity definitions to translate.
  

**Returns**:

  List of translated activities as a ``list[Activity]`` or ``None`` when no input was provided.

### translate\_activity

```python
def translate_activity(
        activity: dict) -> Activity | tuple[Activity, list[Activity]]
```

Translates a single ADF activity into an ``Activity`` object.

**Arguments**:

- `activity` - Activity definition emitted by ADF.
  

**Returns**:

  Translated activity and an optional list of nested activities (for If/ForEach activities).

