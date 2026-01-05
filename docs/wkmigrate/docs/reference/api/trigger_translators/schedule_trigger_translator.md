---
sidebar_label: schedule_trigger_translator
title: wkmigrate.trigger_translators.schedule_trigger_translator
---

This module defines methods for translating Databricks schedule triggers from data pipelines.

### translate\_schedule\_trigger

```python
def translate_schedule_trigger(trigger_definition: dict) -> dict
```

Translates a schedule trigger definition in Data Factory&#x27;s object model to the Databricks SDK cron schedule format.

**Arguments**:

- `trigger_definition` - Schedule trigger definition as a ``dict``.
  

**Returns**:

  Databricks cron schedule definition as a ``dict``.
  

**Raises**:

- `ValueError` - If the trigger definition is missing required properties.

