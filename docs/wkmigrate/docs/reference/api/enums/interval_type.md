---
sidebar_label: interval_type
title: wkmigrate.enums.interval_type
---

Enumerations for scheduling interval units.

## IntervalType Objects

```python
class IntervalType(StrEnum)
```

Schedule interval units supported by pipeline triggers.

Valid options:
    * ``MINUTE``: Run the pipeline on a fixed number of minutes between executions.
    * ``HOUR``: Run the pipeline on an hourly schedule.
    * ``DAY``: Run the pipeline on a daily schedule.
    * ``WEEK``: Run the pipeline on a weekly schedule.
    * ``MONTH``: Run the pipeline on a monthly schedule.

