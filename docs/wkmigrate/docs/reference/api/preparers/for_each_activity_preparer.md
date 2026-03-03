---
sidebar_label: for_each_activity_preparer
title: wkmigrate.preparers.for_each_activity_preparer
---

This module defines a preparer for ForEach activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a ForEach activity. This includes For Each task configuration,
and nested activity tasks and artifacts.

#### prepare\_for\_each\_activity

```python
def prepare_for_each_activity(
        activity: ForEachActivity,
        default_files_to_delta_sinks: bool | None) -> PreparedActivity
```

Builds the task payload for a ForEach activity.

**Arguments**:

- `activity` - Activity definition emitted by the translators
- `default_files_to_delta_sinks` - Optional override for DLT generation
  

**Returns**:

  Prepared activity containing the ForEach task configuration.

