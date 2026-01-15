---
sidebar_label: parsers
title: wkmigrate.translators.trigger_translators.parsers
---

This module defines parsers for translating ADF schedules.

Parsers in this module convert ADF trigger recurrence definitions into quartz cron
expressions used in Databricks Lakeflow jobs. They enforce reasonable defaults and
emit warnings for partially supported configurations.

#### parse\_cron\_expression

```python
def parse_cron_expression(recurrence: dict | None) -> str | None
```

Generates a quartz cron expression from a set of schedule trigger parameters.

**Arguments**:

- `recurrence` - Recurrence object containing the frequency and schedule details.
  

**Returns**:

  Cron expression as a ``str`` or ``None`` when no recurrence is provided.

