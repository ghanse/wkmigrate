---
sidebar_label: parsers
title: wkmigrate.trigger_translators.parsers
---

This module defines methods for parsing trigger objects to the Databricks SDK&#x27;s object model.

### parse\_cron\_expression

```python
def parse_cron_expression(recurrence: dict | None) -> str | None
```

Generates a quartz cron expression from a set of schedule trigger parameters.

**Arguments**:

- `recurrence` - Recurrence object containing the frequency and schedule details.
  

**Returns**:

  Cron expression as a ``str`` or ``None`` when no recurrence is provided.

